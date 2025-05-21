use tonic::{Request, Response, Status};
use tracing::{info, error}; // Added error macro

use crate::state::AppState;
use crate::snapshot_manager::{self}; // Removed SNAPSHOT_MANIFEST_FILE from here
use crate::error::ServerError; // For error handling

// Import the generated gRPC server items
// The actual path will be something like `crate::proto::vortex::api::v1::snapshots_service_server::SnapshotsService`
// and request/response types from `crate::proto::vortex::api::v1`
// For now, using placeholder paths, will adjust after checking generated code or compiler errors.
// Assuming the generated code will be in a module like `crate::vortex_proto::vortex::api::v1`
// based on common practice with tonic-build output structure.
// Corrected paths based on compiler suggestions and typical tonic-build output.
use crate::grpc_api::vortex_api_v1::{
    CreateCollectionSnapshotRequest, CreateCollectionSnapshotResponse,
    DeleteCollectionSnapshotRequest, DeleteCollectionSnapshotResponse,
    ListCollectionSnapshotsRequest, ListCollectionSnapshotsResponse,
    RestoreCollectionSnapshotRequest, RestoreCollectionSnapshotResponse,
    SnapshotDescription, OperationStatus, StatusCode,
    snapshots_service_server::SnapshotsService, // The service trait
};
use std::sync::Arc;
use tokio::sync::RwLock; // Added for RwLock
use chrono::{Utc, DateTime}; // Added DateTime for parsing
use std::fs as std_fs; // Alias to avoid conflict if fs is used elsewhere
use std::path::Path; // Added for Path type in helper function
// Removed unused: use std::path::PathBuf; 

// Helper function to calculate directory size
fn calculate_dir_size(path: &Path) -> Result<u64, std::io::Error> {
    let mut total_size = 0;
    if path.is_dir() {
        for entry in std_fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                total_size += calculate_dir_size(&path)?;
            } else {
                total_size += entry.metadata()?.len();
            }
        }
    } else {
        total_size += std_fs::metadata(path)?.len();
    }
    Ok(total_size)
}

#[derive(Debug)]
pub struct SnapshotsServerImpl {
    pub app_state: Arc<RwLock<AppState>>, // Changed to Arc<RwLock<AppState>>
}

#[tonic::async_trait]
impl SnapshotsService for SnapshotsServerImpl {
    async fn create_collection_snapshot(
        &self,
        request: Request<CreateCollectionSnapshotRequest>,
    ) -> Result<Response<CreateCollectionSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!(collection_name = %req.collection_name, snapshot_name = ?req.snapshot_name, "RPC: create_collection_snapshot");

        let app_state_guard = self.app_state.read().await;
        let live_collection_name = req.collection_name.clone(); 
        let snapshot_name_opt = req.snapshot_name.clone(); // Clone optional snapshot name from the request

        // The directory where all snapshots for all collections are stored.
        let snapshots_root_dir = app_state_guard.data_path.join("snapshots");

        match snapshot_manager::create_collection_snapshot(
            &live_collection_name,
            snapshot_name_opt, // Pass the optional snapshot name
            &*app_state_guard,
            &app_state_guard.data_path,
            &snapshots_root_dir, 
        )
        .await
        {
            Ok(specific_snapshot_path) => { // This is now the path to the specific named snapshot
                // Now, read the manifest from this path to construct SnapshotDescription
                let manifest_path = specific_snapshot_path.join(snapshot_manager::SNAPSHOT_MANIFEST_FILE);
                
                match std_fs::File::open(&manifest_path) {
                    Ok(file) => {
                        match serde_json::from_reader::<_, snapshot_manager::SnapshotManifest>(file) {
                            Ok(manifest) => {
                                let creation_timestamp_prost = DateTime::parse_from_rfc3339(&manifest.timestamp_utc)
                                    .map(|dt| std::time::SystemTime::from(dt.with_timezone(&Utc)))
                                    .map(std::time::SystemTime::into)
                                    .ok();
                                
                                let actual_size_bytes = match calculate_dir_size(&specific_snapshot_path) {
                                    Ok(size) => size as i64,
                                    Err(e) => {
                                        error!("Failed to calculate snapshot size for {:?}: {:?}", specific_snapshot_path, e);
                                        0i64 
                                    }
                                };

                                let snapshot_desc = SnapshotDescription {
                                    snapshot_name: manifest.snapshot_name.clone(), // Now comes from manifest's snapshot_name field
                                    collection_name: manifest.collection_name,
                                    creation_time: creation_timestamp_prost,
                                    size_bytes: actual_size_bytes,
                                    vortex_version: manifest.vortex_version,
                                };

                                Ok(Response::new(CreateCollectionSnapshotResponse {
                                    snapshot_description: Some(snapshot_desc),
                                    status: Some(OperationStatus {
                                        status_code: StatusCode::Ok as i32,
                                        error_message: None,
                                    }),
                                }))
                            }
                            Err(e) => {
                                error!("Failed to parse snapshot manifest {:?}: {:?}", manifest_path, e);
                                Err(Status::internal(format!("Failed to parse snapshot manifest: {}", e)))
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to open snapshot manifest {:?}: {:?}", manifest_path, e);
                        Err(Status::internal(format!("Failed to open snapshot manifest: {}", e)))
                    }
                }
            }
            Err(e) => {
                error!("Failed to create collection snapshot: {:?}", e);
                // Convert ServerError to tonic::Status
                let status = match e {
                    ServerError::IndexNotFound(name) => Status::not_found(format!("Collection '{}' not found", name)),
                    ServerError::CoreError(vortex_err) => Status::internal(format!("Core error: {}", vortex_err)),
                    ServerError::WalError(msg) => Status::internal(format!("WAL error: {}", msg)),
                    ServerError::RocksDBError(msg) => Status::internal(format!("Payload index error: {}", msg)),
                    ServerError::BadRequest(msg) => Status::invalid_argument(msg),
                    ServerError::Internal(msg) => Status::internal(msg),
                    ServerError::LockError(msg) => Status::internal(format!("Lock error: {}", msg)),
                };
                Err(status)
            }
        }
    }

    async fn restore_collection_snapshot(
        &self,
        request: Request<RestoreCollectionSnapshotRequest>,
    ) -> Result<Response<RestoreCollectionSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!(
            target_collection_name = %req.target_collection_name,
            snapshot_name = %req.snapshot_name,
            source_collection_name = %req.source_collection_name,
            "RPC: restore_collection_snapshot"
        );

        let app_state_guard = self.app_state.read().await;
        let snapshot_to_restore_path = app_state_guard.data_path
            .join("snapshots")
            .join(&req.source_collection_name)
            .join(&req.snapshot_name);

        if !snapshot_to_restore_path.exists() || !snapshot_to_restore_path.is_dir() {
            error!("Snapshot directory not found for restore: {:?}", snapshot_to_restore_path);
            return Ok(Response::new(RestoreCollectionSnapshotResponse {
                status: Some(OperationStatus {
                    status_code: StatusCode::NotFound as i32,
                    error_message: Some(format!(
                        "Snapshot '{}' for source collection '{}' not found.",
                        req.snapshot_name, req.source_collection_name
                    )),
                }),
            }));
        }
        
        // The target_persistence_path for restore_collection_snapshot is the app_state.data_path
        // as restore_collection_snapshot will create target_collection_name directory under it.
        match snapshot_manager::restore_collection_snapshot(
            &snapshot_to_restore_path,
            &req.target_collection_name,
            &*app_state_guard, // Pass the guard itself, not a clone of Arc
            &app_state_guard.data_path,
        )
        .await
        {
            Ok(_) => {
                info!(
                    "Successfully restored snapshot '{}' for source collection '{}' to target collection '{}'",
                    req.snapshot_name, req.source_collection_name, req.target_collection_name
                );
                Ok(Response::new(RestoreCollectionSnapshotResponse {
                    status: Some(OperationStatus {
                        status_code: StatusCode::Ok as i32,
                        error_message: None,
                    }),
                }))
            }
            Err(e) => {
                error!(
                    "Failed to restore snapshot '{}' for source collection '{}' to target collection '{}': {:?}",
                    req.snapshot_name, req.source_collection_name, req.target_collection_name, e
                );
                // Convert ServerError to tonic::Status
                let status = match e {
                    ServerError::IndexNotFound(name) => Status::not_found(format!("Collection '{}' not found (during restore pre-check or load)", name)),
                    ServerError::CoreError(vortex_err) => Status::internal(format!("Core error during restore: {}", vortex_err)),
                    ServerError::WalError(msg) => Status::internal(format!("WAL error during restore: {}", msg)),
                    ServerError::RocksDBError(msg) => Status::internal(format!("Payload index error during restore: {}", msg)),
                    ServerError::BadRequest(msg) => Status::invalid_argument(msg), // Should not happen from restore_collection_snapshot directly
                    ServerError::Internal(msg) => Status::internal(msg),
                    ServerError::LockError(msg) => Status::internal(format!("Lock error during restore: {}", msg)),
                };
                Err(status)
            }
        }
    }

    async fn list_collection_snapshots(
        &self,
        request: Request<ListCollectionSnapshotsRequest>,
    ) -> Result<Response<ListCollectionSnapshotsResponse>, Status> {
        let req = request.into_inner();
        info!(collection_name = %req.collection_name, "RPC: list_collection_snapshots");

        let app_state_guard = self.app_state.read().await;
        let collection_snapshots_root_path = app_state_guard.data_path.join("snapshots").join(&req.collection_name);

        let mut snapshots_desc_list = Vec::new();

        if collection_snapshots_root_path.exists() && collection_snapshots_root_path.is_dir() {
            match std_fs::read_dir(&collection_snapshots_root_path) {
                Ok(entries) => {
                    for entry_result in entries {
                        match entry_result {
                            Ok(entry) => {
                                let specific_snapshot_path = entry.path();
                                if specific_snapshot_path.is_dir() {
                                    let manifest_path = specific_snapshot_path.join(snapshot_manager::SNAPSHOT_MANIFEST_FILE);
                                    if manifest_path.exists() {
                                        if let Ok(file) = std_fs::File::open(&manifest_path) {
                                            if let Ok(manifest) = serde_json::from_reader::<_, snapshot_manager::SnapshotManifest>(file) {
                                                // Validate manifest's collection_name
                                                if manifest.collection_name != req.collection_name {
                                                    error!(
                                                        "Manifest collection name '{}' in snapshot '{}' does not match requested collection name '{}'",
                                                        manifest.collection_name, manifest.snapshot_name, req.collection_name
                                                    );
                                                    continue; // Skip this snapshot
                                                }

                                                let creation_timestamp_prost = DateTime::parse_from_rfc3339(&manifest.timestamp_utc)
                                                    .map(|dt| std::time::SystemTime::from(dt.with_timezone(&Utc)))
                                                    .map(std::time::SystemTime::into)
                                                    .ok();

                                                let actual_size_bytes = match calculate_dir_size(&specific_snapshot_path) {
                                                    Ok(size) => size as i64,
                                                    Err(e) => {
                                                        error!("Failed to calculate snapshot size for {:?}: {:?}", specific_snapshot_path, e);
                                                        0i64
                                                    }
                                                };
                                                
                                                snapshots_desc_list.push(SnapshotDescription {
                                                    snapshot_name: manifest.snapshot_name, // From manifest
                                                    collection_name: manifest.collection_name,
                                                    creation_time: creation_timestamp_prost,
                                                    size_bytes: actual_size_bytes,
                                                    vortex_version: manifest.vortex_version,
                                                });
                                            } else {
                                                error!("Failed to parse snapshot manifest {:?}", manifest_path);
                                            }
                                        } else {
                                            error!("Failed to open snapshot manifest {:?}", manifest_path);
                                        }
                                    } else {
                                         info!("Snapshot manifest not found in dir {:?}", specific_snapshot_path);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Error reading entry in snapshot directory {:?}: {}", collection_snapshots_root_path, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read snapshots directory for collection {}: {}", req.collection_name, e);
                    return Err(Status::internal(format!("Failed to list snapshots for collection {}: {}", req.collection_name, e)));
                }
            }
        } else {
            info!("No snapshots directory found for collection {}", req.collection_name);
        }

        Ok(Response::new(ListCollectionSnapshotsResponse {
            snapshots: snapshots_desc_list,
        }))
    }

    async fn delete_collection_snapshot(
        &self,
        request: Request<DeleteCollectionSnapshotRequest>,
    ) -> Result<Response<DeleteCollectionSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!(collection_name = %req.collection_name, snapshot_name = %req.snapshot_name, "RPC: delete_collection_snapshot");

        let app_state_guard = self.app_state.read().await;
        let snapshot_to_delete_path = app_state_guard.data_path
            .join("snapshots")
            .join(&req.collection_name)
            .join(&req.snapshot_name);

        if !snapshot_to_delete_path.exists() {
            error!("Snapshot directory not found for deletion: {:?}", snapshot_to_delete_path);
            return Ok(Response::new(DeleteCollectionSnapshotResponse {
                status: Some(OperationStatus {
                    status_code: StatusCode::NotFound as i32,
                    error_message: Some(format!("Snapshot for collection '{}' not found.", req.collection_name)),
                }),
            }));
        }

        if !snapshot_to_delete_path.is_dir() {
            error!("Path to delete is not a directory: {:?}", snapshot_to_delete_path);
            return Ok(Response::new(DeleteCollectionSnapshotResponse {
                status: Some(OperationStatus {
                    status_code: StatusCode::Error as i32, // Or InvalidArgument if path structure is wrong
                    error_message: Some(format!("Snapshot path for '{}' is not a directory.", req.collection_name)),
                }),
            }));
        }

        match std_fs::remove_dir_all(&snapshot_to_delete_path) {
            Ok(_) => {
                info!("Successfully deleted snapshot directory: {:?}", snapshot_to_delete_path);
                Ok(Response::new(DeleteCollectionSnapshotResponse {
                    status: Some(OperationStatus {
                        status_code: StatusCode::Ok as i32,
                        error_message: None,
                    }),
                }))
            }
            Err(e) => {
                error!("Failed to delete snapshot directory {:?}: {}", snapshot_to_delete_path, e);
                Err(Status::internal(format!("Failed to delete snapshot: {}", e)))
            }
        }
    }
}
