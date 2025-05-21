use crate::state::AppState;
use crate::error::ServerError;
use vortex_core::{Index, VortexError}; // Added Index trait and VortexError
use std::path::{Path, PathBuf};
use std::fs;
use tracing::{info, error, debug, warn}; // Added warn to tracing imports
use serde::{Serialize, Deserialize};
use chrono::Utc; // For timestamping snapshots

pub const SNAPSHOT_MANIFEST_FILE: &str = "snapshot_manifest.json";

#[derive(Serialize, Deserialize, Debug)]
// Made public to be accessible by the gRPC service layer for reading the manifest.
pub struct SnapshotManifest {
    pub snapshot_version: String,
    pub snapshot_name: String, 
    pub collection_name: String,
    pub timestamp_utc: String,
    pub vortex_version: String, 
    pub checkpoint_lsn: Option<u64>,
    pub hnsw_config: Option<vortex_core::HnswConfig>, // Added HNSW config
}

/// Creates a snapshot of a given collection.
///
/// The snapshot will be stored in `snapshot_base_path/collection_name/snapshot_name_to_use/`.
///
/// # Arguments
/// * `collection_name` - The name of the collection to snapshot.
/// * `snapshot_name_override` - Optional user-provided name for the snapshot. If None or empty, a timestamped name is generated.
/// * `app_state` - A reference to the application's shared state.
/// * `persistence_path` - The base path where the live collection data is stored.
/// * `snapshot_base_path` - The root directory where all snapshots are stored (e.g., `data_path/snapshots`).
///
/// # Returns
/// A `Result` indicating success or failure. On success, returns the path to the created snapshot directory.
pub async fn create_collection_snapshot(
    collection_name: &str,
    snapshot_name_override: Option<String>,
    app_state: &AppState,
    persistence_path: &Path,
    snapshot_base_path: &Path, // This is the root for all snapshots, e.g., .../data/snapshots
) -> Result<PathBuf, ServerError> {
    info!(collection_name, ?snapshot_name_override, ?snapshot_base_path, "Attempting to create snapshot for collection");

    let actual_snapshot_name = snapshot_name_override
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            format!(
                "{}_{}",
                collection_name,
                Utc::now().format("%Y%m%d_%H%M%S")
            )
        });
    
    debug!("Determined snapshot name: {}", actual_snapshot_name);

    // Get handles to live components first to check for collection existence
    let hnsw_index_arc = app_state.indices.read().await.get(collection_name).cloned();
    let wal_manager_arc = app_state.wal_managers.read().await.get(collection_name).cloned();
    let payload_idx_arc = app_state.payload_indices.read().await.get(collection_name).cloned();

    if hnsw_index_arc.is_none() || wal_manager_arc.is_none() || payload_idx_arc.is_none() {
        // If any component is missing, the collection is considered not found or incomplete.
        return Err(ServerError::IndexNotFound(collection_name.to_string()));
    }
    
    // Unwrap arcs after confirming they are all Some.
    let hnsw_index_arc = hnsw_index_arc.unwrap();
    let wal_manager_arc = wal_manager_arc.unwrap();
    let payload_idx_arc = payload_idx_arc.unwrap();

    // 1. Validate paths and create snapshot directories (now that collection is confirmed to exist)
    if !persistence_path.exists() || !persistence_path.is_dir() {
        return Err(ServerError::CoreError(VortexError::Configuration(format!("Persistence path {:?} does not exist or is not a directory.", persistence_path))));
    }
    if !snapshot_base_path.exists() {
        fs::create_dir_all(snapshot_base_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: snapshot_base_path.to_path_buf(), // This is the root snapshots dir
            source: e,
        }))?;
    }

    // Snapshots are now stored under snapshot_base_path/collection_name/actual_snapshot_name
    let collection_snapshots_dir = snapshot_base_path.join(collection_name);
    if !collection_snapshots_dir.exists() {
        fs::create_dir_all(&collection_snapshots_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: collection_snapshots_dir.clone(),
            source: e,
        }))?;
    }

    let specific_snapshot_path = collection_snapshots_dir.join(&actual_snapshot_name);
    if specific_snapshot_path.exists() {
        // Fail if specific snapshot dir already exists.
        return Err(ServerError::CoreError(VortexError::Configuration(format!("Specific snapshot directory {:?} already exists.", specific_snapshot_path))));
    }
    fs::create_dir_all(&specific_snapshot_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
        path: specific_snapshot_path.clone(),
        source: e,
    }))?;

    // TODO: Implement a proper "pause" or "prepare for snapshot" mode for the collection
    // For now, we proceed with trying to get consistent state by flushing/checkpointing.
    // Acquire HNSW index write lock early and hold it through critical phases.
    let mut hnsw_index_guard = hnsw_index_arc.write().await;
    let hnsw_config_to_store = hnsw_index_guard.config();

    // 2. Flush HNSW Index and save its state (it saves to its own files)
    debug!(collection_name, "Saving HNSW index state for snapshot...");
    let mut dummy_writer_hnsw = Vec::new(); // save() now manages its own files.
    hnsw_index_guard.save(&mut dummy_writer_hnsw).await?;
    info!(collection_name, "HNSW index state saved successfully for snapshot.");

    // Release HNSW lock after its data is saved to disk, but before long copy operations.
    // The critical part is that HNSW, WAL, and RocksDB checkpoints are made from a consistent state.
    // Ideally, no writes should happen to any component *after* its state is captured and *before* others are.
    // Holding the HNSW lock during WAL/RocksDB checkpointing helps ensure its state doesn't change
    // relative to what WAL/RocksDB will be checkpointed against.

    // 3. Checkpoint WAL (while HNSW is still conceptually "paused" by its lock)
    let mut checkpointed_lsn: Option<u64> = None;
    {
        let wal_manager = wal_manager_arc.clone(); // Clone Arc for this block
        let last_lsn_before_checkpoint = {
            let wal_guard = wal_manager.vortex_wal.lock().await;
            wal_guard.last_lsn()
        };

        if let Some(lsn) = last_lsn_before_checkpoint {
            match wal_manager.checkpoint(lsn).await {
                Ok(_) => {
                    info!(collection_name, "Successfully checkpointed WAL up to LSN {} for snapshot", lsn);
                    checkpointed_lsn = Some(lsn);
                }
                Err(e) => {
                    // Drop HNSW lock before returning error to avoid deadlock if error handling tries to access it.
                    drop(hnsw_index_guard);
                    error!(collection_name, error = ?e, "Failed to checkpoint WAL for snapshot. Snapshot may be inconsistent.");
                    return Err(ServerError::WalError(format!("Failed to checkpoint WAL: {}", e))); // Make this fatal
                }
            }
        } else {
            debug!(collection_name, "WAL is empty or last LSN not determinable; no WAL checkpoint performed for snapshot.");
        }
    }
    
    // 4. Create RocksDB Checkpoint for Payload Index (while HNSW is still "paused")
    let _live_payload_db_path = persistence_path.join(collection_name).join("payload_db");
    let snapshot_payload_db_target_path = specific_snapshot_path.join("payload_db");
    {
        let payload_idx = payload_idx_arc.clone(); // Clone Arc
        if let Err(e) = payload_idx.flush_db_wal() {
             warn!(collection_name, error = ?e, "Failed to flush RocksDB WAL before checkpoint. Proceeding with checkpoint.");
        }
        debug!(collection_name, "Creating RocksDB checkpoint for payload index at {:?}", snapshot_payload_db_target_path);
        if let Err(e) = payload_idx.create_checkpoint(&snapshot_payload_db_target_path) {
            drop(hnsw_index_guard);
            return Err(e);
        }
        info!(collection_name, "RocksDB checkpoint created successfully at {:?}", snapshot_payload_db_target_path);
    }

    // Now that all data components have their state captured (HNSW saved, WAL checkpointed, RocksDB checkpointed),
    // we can release the HNSW index lock before starting file copy operations.
    drop(hnsw_index_guard);
    info!(collection_name, "Released HNSW index lock after data state capture, before file copying.");

    // 5. Copy HNSW data files
    // HNSW data is at persistence_path/collection_name.hnsw_meta.json and persistence_path/collection_name/segment_*/...
    let live_hnsw_meta_file = persistence_path.join(format!("{}.hnsw_meta.json", collection_name));
    let snapshot_hnsw_meta_target_file = specific_snapshot_path.join(format!("{}.hnsw_meta.json", collection_name));
    if live_hnsw_meta_file.exists() {
        fs::copy(&live_hnsw_meta_file, &snapshot_hnsw_meta_target_file).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: snapshot_hnsw_meta_target_file.clone(),
            source: e,
        }))?;
    }

    let live_hnsw_segments_dir = persistence_path.join(collection_name);
    // Segments are copied into a directory named after the collection_name *inside* the specific_snapshot_path
    let snapshot_hnsw_segments_target_dir = specific_snapshot_path.join(collection_name); 
     if live_hnsw_segments_dir.exists() && live_hnsw_segments_dir.is_dir() {
        copy_dir_all(&live_hnsw_segments_dir, &snapshot_hnsw_segments_target_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: snapshot_hnsw_segments_target_dir.clone(), 
            source: e,
        }))?;
        info!(collection_name, "HNSW segment data copied successfully.");
    }


    // 6. Copy WAL files (up to checkpointed_lsn if possible, or all current ones)
    let live_wal_dir = persistence_path.join(collection_name).join("wal");
    let snapshot_wal_target_dir = specific_snapshot_path.join("wal");
    if live_wal_dir.exists() && live_wal_dir.is_dir() {
        fs::create_dir_all(&snapshot_wal_target_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: snapshot_wal_target_dir.clone(), 
            source: e,
        }))?;
        // Note on selective WAL copy: The `wal_manager.checkpoint()` call earlier uses `prefix_truncate_log_until_lsn`,
        // which removes WAL segments entirely covered by the checkpoint. Thus, `copy_dir_all` here copies
        // the remaining relevant segments (those active at/after checkpoint_lsn and the open segment).
        // This achieves a selective copy.
        copy_dir_all(&live_wal_dir, &snapshot_wal_target_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: snapshot_wal_target_dir.clone(),
            source: e,
        }))?;
        info!(collection_name, "WAL files copied successfully.");
    }

    // 7. Create Snapshot Manifest
    let manifest = SnapshotManifest {
        snapshot_version: "1.0.0".to_string(),
        snapshot_name: actual_snapshot_name.clone(), 
        collection_name: collection_name.to_string(),
            timestamp_utc: Utc::now().to_rfc3339(),
            vortex_version: env!("CARGO_PKG_VERSION").to_string(),
            checkpoint_lsn: checkpointed_lsn,
            hnsw_config: Some(hnsw_config_to_store),
        };

    let manifest_path = specific_snapshot_path.join(SNAPSHOT_MANIFEST_FILE);
    let file = fs::File::create(&manifest_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
        path: manifest_path.clone(),
        source: e,
    }))?;
    serde_json::to_writer_pretty(file, &manifest).map_err(|e| ServerError::CoreError(VortexError::Serialization(
        format!("Failed to serialize snapshot manifest: {}", e)
    )))?;
    info!(collection_name, snapshot_name = actual_snapshot_name, "Snapshot manifest created successfully at {:?}", manifest_path);

    info!(collection_name, snapshot_name = actual_snapshot_name, snapshot_path = ?specific_snapshot_path, "Snapshot created successfully.");
    Ok(specific_snapshot_path)
}


// Helper function to copy directory contents recursively
// Source: https://stackoverflow.com/questions/26958489/how-to-copy-a-folder-recursively-in-rust
fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// Restores a collection from a snapshot.
///
/// # Arguments
/// * `snapshot_collection_path` - Path to the specific collection's snapshot directory (e.g., /path/to/snapshots/my_collection).
/// * `collection_name_to_restore_as` - The name for the collection once restored.
/// * `app_state` - A reference to the application's shared state.
/// * `target_persistence_path` - The base path where the live collection data will be restored.
///
/// # Returns
/// A `Result` indicating success or failure.
pub async fn restore_collection_snapshot(
    snapshot_collection_path: &Path,
    collection_name_to_restore_as: &str,
    app_state: &AppState,
    target_persistence_path: &Path,
) -> Result<(), ServerError> {
    info!(?snapshot_collection_path, collection_name_to_restore_as, "Attempting to restore collection from snapshot");

    // 1. Validate snapshot path and read manifest
    if !snapshot_collection_path.exists() || !snapshot_collection_path.is_dir() {
        return Err(ServerError::CoreError(VortexError::Configuration(format!(
            "Snapshot path {:?} does not exist or is not a directory.", snapshot_collection_path
        ))));
    }

    let manifest_path = snapshot_collection_path.join(SNAPSHOT_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Err(ServerError::CoreError(VortexError::Configuration(format!(
            "Snapshot manifest file {:?} not found.", manifest_path
        ))));
    }
    let manifest_file = fs::File::open(&manifest_path).map_err(|e| ServerError::CoreError(VortexError::IoError {
        path: manifest_path.clone(),
        source: e,
    }))?;
    let manifest: SnapshotManifest = serde_json::from_reader(manifest_file).map_err(|e| ServerError::CoreError(VortexError::Deserialization(
        format!("Failed to deserialize snapshot manifest {:?}: {}", manifest_path, e)
    )))?;

    // Basic validation (can be expanded, e.g., version compatibility)
    // For now, we assume collection_name_to_restore_as is the name to use.
    // The manifest.collection_name is the original name from the snapshot.
    info!(collection_name_to_restore_as, manifest = ?manifest, "Snapshot manifest loaded.");

    // 2. Pre-checks for target
    let live_collection_base_path = target_persistence_path.join(collection_name_to_restore_as);
    if live_collection_base_path.exists() {
        // For now, fail if target collection dir already exists.
        // Later, could add overwrite policy or load if compatible.
        return Err(ServerError::CoreError(VortexError::Configuration(format!(
            "Target collection directory {:?} already exists. Restore aborted.", live_collection_base_path
        ))));
    }
     // Also check in-memory state
    if app_state.indices.read().await.contains_key(collection_name_to_restore_as) {
        return Err(ServerError::CoreError(VortexError::Configuration(format!(
            "Collection '{}' already exists in memory. Restore aborted.", collection_name_to_restore_as
        ))));
    }


    // 3. Create target directories
    fs::create_dir_all(&live_collection_base_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
        path: live_collection_base_path.clone(), source: e
    }))?;
    
    let live_hnsw_segments_dir = target_persistence_path.join(collection_name_to_restore_as); // Segments go into collection_name_to_restore_as/
    // No need to create live_hnsw_segments_dir explicitly if copy_dir_all handles it for the parent.
    // fs::create_dir_all(&live_hnsw_segments_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
    // path: live_hnsw_segments_dir.clone(), source: e
    // }))?;


    let live_payload_db_path = live_collection_base_path.join("payload_db");
    fs::create_dir_all(&live_payload_db_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
        path: live_payload_db_path.clone(), source: e
    }))?;

    let live_wal_dir = live_collection_base_path.join("wal");
    fs::create_dir_all(&live_wal_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
        path: live_wal_dir.clone(), source: e
    }))?;


    // 4. Restore data from snapshot to target persistence path
    
    // Restore HNSW meta file (e.g., collection_name.hnsw_meta.json)
    // The manifest's collection_name is the original name. We are restoring as collection_name_to_restore_as.
    let snapshot_hnsw_meta_file = snapshot_collection_path.join(format!("{}.hnsw_meta.json", manifest.collection_name));
    let target_hnsw_meta_file = target_persistence_path.join(format!("{}.hnsw_meta.json", collection_name_to_restore_as));
    if snapshot_hnsw_meta_file.exists() {
        fs::copy(&snapshot_hnsw_meta_file, &target_hnsw_meta_file).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: target_hnsw_meta_file.clone(), source: e,
        }))?;
        debug!("Restored HNSW meta file to {:?}", target_hnsw_meta_file);
    } else {
         warn!("Snapshot HNSW meta file {:?} not found. HNSWIndex::open might rely on defaults or fail.", snapshot_hnsw_meta_file);
    }

    // Restore HNSW segments directory (e.g., snapshot/my_collection_original/segment_X -> live/my_collection_new/segment_X)
    let snapshot_hnsw_segments_dir = snapshot_collection_path.join(&manifest.collection_name); // Segments are in a dir named after original collection name
    if snapshot_hnsw_segments_dir.exists() && snapshot_hnsw_segments_dir.is_dir() {
        copy_dir_all(&snapshot_hnsw_segments_dir, &live_hnsw_segments_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: live_hnsw_segments_dir.clone(), source: e,
        }))?;
        debug!("Restored HNSW segments to {:?}", live_hnsw_segments_dir);
    } else {
        warn!("Snapshot HNSW segments directory {:?} not found or not a directory.", snapshot_hnsw_segments_dir);
    }

    // Restore Payload DB (RocksDB)
    let snapshot_payload_db_dir = snapshot_collection_path.join("payload_db");
    if snapshot_payload_db_dir.exists() && snapshot_payload_db_dir.is_dir() {
        copy_dir_all(&snapshot_payload_db_dir, &live_payload_db_path).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: live_payload_db_path.clone(), source: e,
        }))?;
        debug!("Restored Payload DB to {:?}", live_payload_db_path);
    } else {
        return Err(ServerError::CoreError(VortexError::Configuration(format!(
            "Snapshot payload_db directory {:?} not found.", snapshot_payload_db_dir
        ))));
    }
    
    // Restore WAL Data
    let snapshot_wal_dir = snapshot_collection_path.join("wal");
    if snapshot_wal_dir.exists() && snapshot_wal_dir.is_dir() {
        copy_dir_all(&snapshot_wal_dir, &live_wal_dir).map_err(|e| ServerError::CoreError(VortexError::IoError{
            path: live_wal_dir.clone(), source: e,
        }))?;
        debug!("Restored WAL files to {:?}", live_wal_dir);
    } else {
         warn!("Snapshot WAL directory {:?} not found. Collection will start with fresh WAL.", snapshot_wal_dir);
    }

    // 5. Load restored collection into AppState
    // This part is critical and mirrors parts of persistence::load_all_indices_on_startup
    
    // Use HNSW config from manifest if available, otherwise use a default.
    // The HnswIndex::open method itself will also try to load .hnsw_meta.json which contains config.
    // The manifest.hnsw_config provides an explicit config from the time of snapshot.
    // HnswIndex::open's default_config is a fallback if its own metadata is also missing.
        let config_for_open = manifest.hnsw_config.unwrap_or_else(|| {
            warn!("HNSW config not found in snapshot manifest for {}, using default.", manifest.snapshot_name);
            // Ensure vector_dim is set if using default, though HnswIndex::open should prioritize its own meta.
            let dc = vortex_core::HnswConfig::default();
            // If manifest.collection_name's HNSW index had a specific dim, we don't know it here if not in manifest.
            // HnswIndex::open will load its own .hnsw_meta.json which should have the correct dim.
        // So, this default_config's vector_dim is less critical if .hnsw_meta.json is present.
        if dc.vector_dim == 0 && manifest.hnsw_config.is_none() {
             warn!("Default HNSWConfig has 0 vector_dim, HNSWIndex::open must find it in its own metadata.");
        }
        dc
    });
    // Metric is also part of HnswIndex's own metadata. For now, use a common default for open.
    let default_metric = vortex_core::DistanceMetric::Cosine; 

    let hnsw_index = vortex_core::HnswIndex::open(
        target_persistence_path, 
        collection_name_to_restore_as, 
        config_for_open, // This config is used by HnswIndex::open if its own .hnsw_meta.json is missing/corrupt
        default_metric   // Same for metric
    )
        .await
        .map_err(ServerError::from)?;
    info!(collection_name_to_restore_as, "Restored HNSW index core data loaded successfully.");

    let payload_idx_db = crate::payload_index::PayloadIndexRocksDB::new(&live_payload_db_path)?;
    info!(collection_name_to_restore_as, "Restored PayloadIndexRocksDB initialized successfully.");
    
    use crate::wal::wal_manager::CollectionWalManager;
    use crate::wal::VortexWalOptions;

    let wal_manager = CollectionWalManager::new(live_wal_dir.clone(), VortexWalOptions::default())?;
    // IMPORTANT: After restoring a snapshot, the WAL should be initialized to a state
    // consistent with the checkpoint_lsn. New writes should start *after* this LSN.
    // The current CollectionWalManager::new might try to recover from existing WAL files.
    // We might need a specific method in WAL manager to initialize post-restore,
    // or ensure it correctly handles the restored WAL files and checkpoint_lsn.
    // For now, let's assume new() correctly sets up for future writes based on existing files.
    // If manifest.checkpoint_lsn is Some(lsn), the WAL manager should know that operations up to lsn are already reflected.
    // This might involve setting the WAL's internal "last_synced_lsn" or similar.
    // This part needs careful review of WAL behavior.
    // A simple approach: after restoring WAL files, the WAL manager on `new` should read the last LSN from these files.
    // If the snapshot was consistent, no WAL replay is needed from these restored files.
    info!(collection_name_to_restore_as, checkpoint_lsn = ?manifest.checkpoint_lsn, "Restored CollectionWalManager initialized. Ensure WAL state is consistent with checkpoint LSN.");


    // Lock AppState components for writing
    let mut indices_map_guard = app_state.indices.write().await;
    let mut payload_indices_guard = app_state.payload_indices.write().await;
    let mut wal_managers_guard = app_state.wal_managers.write().await;
    // Note: metadata_store (legacy HashMap payloads) is not restored as PayloadIndexRocksDB is primary.

    indices_map_guard.insert(collection_name_to_restore_as.to_string(), std::sync::Arc::new(tokio::sync::RwLock::new(hnsw_index)));
    payload_indices_guard.insert(collection_name_to_restore_as.to_string(), std::sync::Arc::new(payload_idx_db));
    wal_managers_guard.insert(collection_name_to_restore_as.to_string(), std::sync::Arc::new(wal_manager));
    
    info!(collection_name_to_restore_as, "Collection successfully restored and loaded into application state.");
    Ok(())
}
