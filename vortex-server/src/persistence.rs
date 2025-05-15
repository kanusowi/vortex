use crate::state::AppState;
use crate::error::{ServerError, ServerResult}; 
use crate::wal::wal_manager::{CollectionWalManager, WalRecord};
use crate::wal::VortexWalOptions;
use vortex_core::{VortexError, VectorId};
use crate::payload_index::PayloadIndexRocksDB; // Added
use serde::{Serialize, Deserialize}; // Added Serialize, Deserialize
use std::collections::HashMap; // Added HashMap
use std::fs::{self, File};
use std::io::{BufWriter, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock; // Not strictly needed here if AppState is passed by ref
use tracing::{info, warn, debug}; // Removed 'error'
use vortex_core::{HnswIndex, Index}; // Index trait for HnswIndex methods

const HNSW_INDEX_FILE_EXT: &str = "vortex"; // This seems unused now as HnswIndex::open/save manage their own files
const METADATA_FILE_EXT: &str = "meta.json";

// Helper struct for server-side metadata including checkpoint LSN
#[derive(Serialize, Deserialize, Debug, Default)]
struct IndexServerMetadata {
    #[serde(rename = "_checkpoint_lsn", skip_serializing_if = "Option::is_none")]
    checkpoint_lsn: Option<u64>,
    #[serde(flatten)]
    payloads: HashMap<VectorId, serde_json::Value>,
}


fn get_hnsw_file_path(dir: &Path, index_name: &str) -> PathBuf {
    // HnswIndex::open and save now take base_path and index_name, constructing internal paths.
    // This function might be less relevant for direct HNSW file paths.
    // For now, let's assume it points to a conceptual directory or a primary file if needed.
    // However, HnswIndex manages its own .vec, .del, .graph, .hnsw_meta.json files.
    // This function is not directly used by HnswIndex::save/open.
    dir.join(format!("{}.{}", index_name, HNSW_INDEX_FILE_EXT)) // Retaining for now, but likely unused for HNSW data.
}

fn get_metadata_file_path(dir: &Path, index_name: &str) -> PathBuf {
    dir.join(format!("{}.{}", index_name, METADATA_FILE_EXT))
}

/// Saves a single index (both HNSW data and metadata) to disk.
pub async fn save_index(index_name: &str, app_state: &AppState, persistence_path: &Path) -> ServerResult<()> {
    info!(index_name, path = ?persistence_path, "Attempting to save index");

    // Ensure persistence directory exists
    fs::create_dir_all(persistence_path)
        .map_err(|e| VortexError::IoError { path: persistence_path.to_path_buf(), source: e })?;

    let mut successfully_checkpointed_lsn: Option<u64> = None;

    // --- Block for HNSW Index Saving and WAL Checkpointing ---
    {
        let indices_map = app_state.indices.read().await;
        if let Some(index_arc_lock) = indices_map.get(index_name) {
            let mut index_guard = index_arc_lock.write().await;
            let hnsw_file_path = get_hnsw_file_path(persistence_path, index_name); // Path for logging
            debug!(file_path = ?hnsw_file_path, "Saving HNSW data for index");
            
            let mut dummy_writer = Vec::new(); // HnswIndex::save now manages its own files.
            index_guard.save(&mut dummy_writer).await.map_err(ServerError::from)?;
            info!(index_name, "HNSW data and its internal metadata saved successfully.");

            let wal_managers_map = app_state.wal_managers.read().await;
            if let Some(wal_manager) = wal_managers_map.get(index_name) {
                let last_lsn_before_checkpoint = {
                    let wal_guard = wal_manager.vortex_wal.lock().await;
                    wal_guard.last_lsn()
                };

                if let Some(lsn) = last_lsn_before_checkpoint {
                    match wal_manager.checkpoint(lsn).await { // Added .await
                        Ok(_) => {
                            info!(index_name, "Successfully checkpointed WAL up to LSN {}", lsn);
                            successfully_checkpointed_lsn = Some(lsn); // Capture the LSN that was checkpointed
                        }
                        Err(e) => {
                            tracing::error!(index_name, error = ?e, "Failed to checkpoint WAL for index");
                        }
                    }
                } else {
                    tracing::warn!(index_name, "Could not determine last LSN for WAL checkpointing or WAL is empty.");
                }
            } else {
                tracing::warn!(index_name, "WAL manager not found for index, cannot checkpoint WAL.");
            }
        } else {
            tracing::warn!(index_name, "Index not found in memory, cannot save HNSW data.");
            return Err(ServerError::IndexNotFound(index_name.to_string()));
        }
    } // HNSW index_guard and other read locks are released here.

    // --- Block for Saving Server-Side Payload Metadata ---
    let metadata_file_path = get_metadata_file_path(persistence_path, index_name);
    {
        let payloads = app_state.metadata_store.read().await.get(index_name).cloned().unwrap_or_default();
        // Use the `successfully_checkpointed_lsn` captured from the WAL checkpointing step.

        let server_metadata = IndexServerMetadata {
            checkpoint_lsn: successfully_checkpointed_lsn,
            payloads,
        };

        // Save even if payloads are empty, to store checkpoint_lsn if present.
        // Or, only save if checkpoint_lsn is Some or payloads are not empty.
        // Let's save if either is true.
        if server_metadata.checkpoint_lsn.is_some() || !server_metadata.payloads.is_empty() {
            debug!(file_path = ?metadata_file_path, "Saving server metadata for index");
            let file = File::create(&metadata_file_path)
                .map_err(|e| VortexError::IoError { path: metadata_file_path.clone(), source: e })?;
            serde_json::to_writer_pretty(BufWriter::new(file), &server_metadata)
                .map_err(|e| {
                    tracing::error!(index_name, error = ?e, "Failed to serialize server metadata");
                    VortexError::Serialization(format!("Failed to serialize server metadata for {}: {}", index_name, e))
                })?;
            info!(index_name, file_path = ?metadata_file_path, "Server metadata saved successfully");
        } else {
            debug!(index_name, "No server metadata (payloads or checkpoint LSN) to save. Skipping metadata file creation.");
            if metadata_file_path.exists() {
                fs::remove_file(&metadata_file_path).map_err(|e| VortexError::IoError { path: metadata_file_path.clone(), source: e})?;
                debug!(index_name, file_path = ?metadata_file_path, "Removed empty/stale server metadata file.");
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
                tracing::error!(index_name, error = ?e, "Failed to save index.");
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
            let mut wal_managers_guard = app_state.wal_managers.write().await;
            let mut payload_indices_guard = app_state.payload_indices.write().await; // Lock for payload_indices

            for entry_result in entries {
                match entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        // We are looking for the primary HNSW data file (e.g., .vec) to identify an index directory
                        // The HNSW_INDEX_FILE_EXT was ".vortex" which was for bincode.
                        // Now, HnswIndex::open uses base_path and name.
                        // We should iterate directories in persistence_path, or look for a marker file like .hnsw_meta.json
                        // For now, let's assume each index has its files directly in persistence_path (e.g., my_index.vec, my_index.graph, my_index.hnsw_meta.json)
                        // And we identify an index by its .hnsw_meta.json file.
                        if path.is_file() && path.file_name().map_or(false, |name| name.to_string_lossy().ends_with(".hnsw_meta.json")) {
                            if let Some(file_stem_os) = path.file_stem() { // e.g., "my_index.hnsw_meta" or "test-index-wal.hnsw_meta"
                                let full_file_stem = file_stem_os.to_string_lossy();
                                
                                // Corrected logic to derive index_name by stripping ".hnsw_meta" suffix
                                let index_name = if let Some(name_part) = full_file_stem.strip_suffix(".hnsw_meta") {
                                    name_part.to_string()
                                } else {
                                    warn!("Could not derive index name from metadata file stem: {} (from file: {:?})", full_file_stem, path);
                                    continue;
                                };
                                
                                info!(%index_name, meta_file_path = ?path, "Found potential index metadata file. Attempting to load index.");

                                let default_config = vortex_core::HnswConfig::default(); 
                                let default_metric = vortex_core::DistanceMetric::Cosine;
                                
                                match HnswIndex::open(persistence_path, &index_name, default_config, default_metric).await { // Added .await
                                    Ok(mut hnsw_index) => { // hnsw_index is mutable for WAL replay
                                        info!(%index_name, config=?hnsw_index.config(), metric=?hnsw_index.distance_metric(), "HNSW index core data loaded successfully.");
                                        
                                        // Load server-side metadata to get checkpoint_lsn
                                        let metadata_file_path = get_metadata_file_path(persistence_path, &index_name);
                                        let mut loaded_server_metadata = IndexServerMetadata::default();

                                        if metadata_file_path.exists() {
                                            match File::open(&metadata_file_path) {
                                                Ok(file) => {
                                                    match serde_json::from_reader(BufReader::new(file)) {
                                                        Ok(meta) => {
                                                            loaded_server_metadata = meta;
                                                            info!(index_name, "Successfully loaded server metadata (payloads and checkpoint LSN).");
                                                        }
                                                        Err(e) => {
                                                            tracing::error!(index_name, file_path = ?metadata_file_path, error = ?e, "Failed to deserialize server metadata. Proceeding without it.");
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::warn!(index_name, file_path = ?metadata_file_path, error = ?e, "Failed to open server metadata file. Proceeding without it.");
                                                }
                                            }
                                        } else {
                                            info!(index_name, "No server metadata file found. Proceeding without it.");
                                        }
                                        
                                        // Store payloads in AppState
                                        metadata_store_guard.insert(index_name.clone(), loaded_server_metadata.payloads.clone());
                                        // The checkpoint_lsn is now directly part of loaded_server_metadata.
                                        // No need to write to a temporary AppState.last_checkpointed_lsns during load.

                                        // WAL Recovery, now using the loaded checkpoint_lsn
                                        let wal_dir_path = CollectionWalManager::get_wal_path_for_index(persistence_path, &index_name);
                                        match CollectionWalManager::new(wal_dir_path.clone(), VortexWalOptions::default()) {
                                            Ok(wal_manager) => {
                                                let checkpoint_lsn_to_recover_after = loaded_server_metadata.checkpoint_lsn;
                                                info!(%index_name, "Attempting WAL recovery (after LSN {:?})...", checkpoint_lsn_to_recover_after);
                                                match wal_manager.recover_from_wal(checkpoint_lsn_to_recover_after).await { // Added .await
                                                    Ok(wal_records) => {
                                                        if !wal_records.is_empty() {
                                                            info!(%index_name, num_records = wal_records.len(), "Replaying WAL records (after LSN {:?})...", checkpoint_lsn_to_recover_after);
                                                            for (lsn, record) in wal_records {
                                                                debug!(%index_name, %lsn, ?record, "Replaying WAL record");
                                                                // Apply record to hnsw_index
                                                                // This needs to be done carefully and idempotently if possible
                                                                match record {
                                                                    WalRecord::AddVector { vector_id, vector, metadata } => {
                                                                        // Replay vector addition to HnswIndex
                                                                        if let Err(e) = hnsw_index.add_vector(vector_id.clone(), vector).await {
                                                                            tracing::error!(%index_name, %lsn, error = ?e, "Error replaying AddVector to HnswIndex from WAL");
                                                                        }
                                                                        // Replay metadata addition to metadata_store
                                                                        if let Some(meta_val) = metadata {
                                                                            metadata_store_guard
                                                                                .entry(index_name.clone())
                                                                                .or_default()
                                                                                .insert(vector_id.clone(), meta_val);
                                                                            debug!(%index_name, %lsn, %vector_id, "Replayed AddVector metadata from WAL.");
                                                                        }
                                                                    }
                                                                    WalRecord::DeleteVector { vector_id, .. } => {
                                                                        // Replay vector deletion from HnswIndex
                                                                        if let Err(e) = hnsw_index.delete_vector(&vector_id).await {
                                                                            tracing::error!(%index_name, %lsn, error = ?e, "Error replaying DeleteVector from HnswIndex from WAL");
                                                                        }
                                                                        // Replay metadata deletion from metadata_store
                                                                        if let Some(index_payloads) = metadata_store_guard.get_mut(&index_name) {
                                                                            if index_payloads.remove(&vector_id).is_some() {
                                                                                debug!(%index_name, %lsn, %vector_id, "Replayed DeleteVector metadata from WAL.");
                                                                            }
                                                                        }
                                                                    }
                                                                    WalRecord::CreateIndex { .. } => {
                                                                        // Usually, CreateIndex is for initial setup.
                                                                        // If we are recovering an existing index, this might be a no-op or for validation.
                                                                        debug!(%index_name, %lsn, "Skipping CreateIndex record during WAL replay for already opened index.");
                                                                    }
                                                                }
                                                            }
                                                            info!(%index_name, "WAL replay completed.");
                                                        } else {
                                                            info!(%index_name, "No WAL records to replay.");
                                                        }
                                                        wal_managers_guard.insert(index_name.clone(), Arc::new(wal_manager));
                                                    }
                                                    Err(e) => {
                                                        tracing::error!(%index_name, error = ?e, "Failed to recover from WAL. Index may be stale.");
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                tracing::error!(%index_name, error = ?e, "Failed to initialize WAL manager. Index loaded without WAL recovery.");
                                            }
                                        }
                                        indices_map_guard.insert(index_name.clone(), Arc::new(RwLock::new(hnsw_index)));
                                        loaded_count += 1;
                                        // Metadata (payloads) already loaded and stored above.
                                        // No separate error count for metadata now, as it's part of IndexServerMetadata.

                                        // Initialize PayloadIndexRocksDB
                                        let index_data_path = persistence_path.join(&index_name);
                                        let payload_db_path = index_data_path.join("payload_db");
                                        match PayloadIndexRocksDB::new(&payload_db_path) {
                                            Ok(payload_idx_db) => {
                                                payload_indices_guard.insert(index_name.clone(), Arc::new(payload_idx_db));
                                                info!(index_name, path=?payload_db_path, "PayloadIndexRocksDB initialized successfully.");
                                            }
                                            Err(e) => {
                                                tracing::error!(index_name, path=?payload_db_path, error=?e, "Failed to initialize PayloadIndexRocksDB for index. Payloads may not be available.");
                                                // Decide if this is a fatal error for loading the index or just a warning.
                                                // For now, let the index load without payload DB if it fails.
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(index_name, file_path = ?path, error = ?e, "Failed to load HNSW index data.");
                                        error_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(path = ?persistence_path, error = ?e, "Error reading directory entry.");
                        error_count += 1;
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(path = ?persistence_path, error = ?e, "Failed to read persistence directory.");
            // No indices loaded if directory can't be read.
        }
    }
    info!(loaded_count, error_count, "Finished loading indices from disk.");
}

// Removed manual From<VortexError> for ServerError implementation,
// as it's handled by #[from] in ServerError::CoreError(VortexError)
