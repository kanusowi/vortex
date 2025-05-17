use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn, error};

use crate::state::AppState;
use crate::grpc_api::vortex_api_v1::{
    collections_service_server::CollectionsService,
    CreateCollectionRequest, CreateCollectionResponse,
    GetCollectionInfoRequest, GetCollectionInfoResponse,
    ListCollectionsRequest, ListCollectionsResponse,
    DeleteCollectionRequest, DeleteCollectionResponse,
    CollectionDescription, CollectionStatus,
    // HnswConfigParams is part of CreateCollectionRequest
    // DistanceMetric is part of CreateCollectionRequest
};
use crate::wal::wal_manager::{CollectionWalManager, WalRecord};
use crate::wal::VortexWalOptions;
use crate::payload_index::PayloadIndexRocksDB;
use vortex_core::{HnswIndex, Index, VortexError}; // Removed HnswConfig (unused)

// Helper to convert gRPC DistanceMetric to core DistanceMetric
fn proto_to_core_distance_metric(
    proto_metric_i32: i32,
) -> Result<vortex_core::DistanceMetric, Status> {
    // Use try_from instead of deprecated from_i32
    match crate::grpc_api::vortex_api_v1::DistanceMetric::try_from(proto_metric_i32) {
        Ok(crate::grpc_api::vortex_api_v1::DistanceMetric::Cosine) => Ok(vortex_core::DistanceMetric::Cosine),
        Ok(crate::grpc_api::vortex_api_v1::DistanceMetric::EuclideanL2) => Ok(vortex_core::DistanceMetric::L2),
        Ok(crate::grpc_api::vortex_api_v1::DistanceMetric::Unspecified) => { // Handle Unspecified explicitly
            Err(Status::invalid_argument("Distance metric cannot be unspecified. Please choose Cosine or EuclideanL2."))
        }
        Err(_) => Err(Status::invalid_argument(format!("Invalid distance metric value: {}", proto_metric_i32))),
    }
}

// Helper to convert core DistanceMetric to gRPC DistanceMetric
fn core_to_proto_distance_metric(
    core_metric: vortex_core::DistanceMetric,
) -> crate::grpc_api::vortex_api_v1::DistanceMetric {
    match core_metric {
        vortex_core::DistanceMetric::Cosine => crate::grpc_api::vortex_api_v1::DistanceMetric::Cosine,
        vortex_core::DistanceMetric::L2 => crate::grpc_api::vortex_api_v1::DistanceMetric::EuclideanL2,
    }
}

// Helper to convert core HnswConfig to gRPC HnswConfigParams
fn core_to_proto_hnsw_config_params(
    core_config: &vortex_core::HnswConfig,
) -> crate::grpc_api::vortex_api_v1::HnswConfigParams {
    crate::grpc_api::vortex_api_v1::HnswConfigParams {
        m: core_config.m as u32,
        m_max0: core_config.m_max0 as u32,
        ef_construction: core_config.ef_construction as u32,
        ef_search: core_config.ef_search as u32,
        ml: core_config.ml,
        seed: core_config.seed,
        vector_dim: core_config.vector_dim,
    }
}

#[derive(Debug)]
pub struct CollectionsServerImpl {
    pub app_state: Arc<RwLock<AppState>>,
}

#[tonic::async_trait]
impl CollectionsService for CollectionsServerImpl {
    async fn create_collection(
        &self,
        request: Request<CreateCollectionRequest>,
    ) -> Result<Response<CreateCollectionResponse>, Status> {
        let req_inner = request.into_inner();
        info!(
            collection_name = %req_inner.collection_name,
            dimensions = req_inner.vector_dimensions,
            metric = ?req_inner.distance_metric,
            "RPC: CreateCollection received"
        );

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }
        if req_inner.vector_dimensions == 0 {
            return Err(Status::invalid_argument(
                "Vector dimensions must be greater than 0",
            ));
        }

        let core_metric = proto_to_core_distance_metric(req_inner.distance_metric)?;
        let requested_vector_dim = req_inner.vector_dimensions;

        let core_hnsw_config = if let Some(params_proto) = req_inner.hnsw_config {
            if params_proto.vector_dim != 0 && params_proto.vector_dim != requested_vector_dim {
                return Err(Status::invalid_argument(format!(
                    "Mismatch between CreateCollectionRequest.vector_dimensions ({}) and HnswConfigParams.vector_dim ({})",
                    requested_vector_dim, params_proto.vector_dim
                )));
            }
            vortex_core::HnswConfig {
                m: params_proto.m as usize,
                m_max0: params_proto.m_max0 as usize,
                ef_construction: params_proto.ef_construction as usize,
                ef_search: params_proto.ef_search as usize,
                ml: params_proto.ml,
                seed: params_proto.seed,
                vector_dim: requested_vector_dim,
            }
        } else {
            let mut default_config = vortex_core::HnswConfig::default();
            default_config.vector_dim = requested_vector_dim;
            default_config
        };

        core_hnsw_config.validate().map_err(|e: VortexError| {
            Status::invalid_argument(format!("Invalid HNSW config: {}", e))
        })?;

        let app_state_guard = self.app_state.read().await;
        let mut indices_map_writer = app_state_guard.indices.write().await;

        if indices_map_writer.contains_key(&req_inner.collection_name) {
            warn!(collection_name = %req_inner.collection_name, "Attempted to create existing collection via gRPC");
            return Err(Status::already_exists(format!(
                "Collection '{}' already exists",
                req_inner.collection_name
            )));
        }

        let data_path_buf = app_state_guard.data_path.clone();
        let base_path = &data_path_buf;

        let new_index = HnswIndex::new(
            base_path,
            &req_inner.collection_name,
            core_hnsw_config,
            core_metric,
        )
        .await
        .map_err(|e| Status::internal(format!("Failed to create HNSW index: {}", e)))?;

        indices_map_writer.insert(req_inner.collection_name.clone(), Arc::new(RwLock::new(new_index)));
        drop(indices_map_writer);

        let wal_path = CollectionWalManager::get_wal_path_for_index(&data_path_buf, &req_inner.collection_name);
        let wal_manager = CollectionWalManager::new(wal_path, VortexWalOptions::default())
            .map_err(|e| Status::internal(format!("Failed to create WAL manager: {:?}", e)))?;
        
        let create_index_record = WalRecord::CreateIndex {
            index_name: req_inner.collection_name.clone(),
            config: core_hnsw_config,
            metric: core_metric,
            dimensions: requested_vector_dim,
            // capacity: DEFAULT_CAPACITY, // Removed as capacity is no longer part of WalRecord::CreateIndex
        };

        if let Err(e) = wal_manager.log_operation(&create_index_record).await {
            error!(collection_name = %req_inner.collection_name, error = ?e, "CRITICAL: Failed to log CreateCollection to WAL after index files were created. Cleaning up HNSW index files.");
            // Attempt to remove the HNSW index files from disk
            let collection_disk_path = data_path_buf.join(&req_inner.collection_name);
            if let Err(cleanup_err) = tokio::fs::remove_dir_all(&collection_disk_path).await {
                error!(collection_name = %req_inner.collection_name, path = ?collection_disk_path, error = ?cleanup_err, "Failed to cleanup collection directory after WAL logging failure.");
                // Log this error, but proceed to remove from in-memory map and return original WAL error
            }
            app_state_guard.indices.write().await.remove(&req_inner.collection_name);
            return Err(Status::internal(format!("Failed to log CreateCollection to WAL: {:?}. Associated index files attempted to be cleaned up.", e)));
        }

        let mut wal_managers_map_writer = app_state_guard.wal_managers.write().await;
        wal_managers_map_writer.insert(req_inner.collection_name.clone(), Arc::new(wal_manager));
        drop(wal_managers_map_writer);

        let payload_db_path = data_path_buf.join(&req_inner.collection_name).join("payload_db");
        match PayloadIndexRocksDB::new(&payload_db_path) {
            Ok(payload_idx_db) => {
                let mut payload_indices_guard = app_state_guard.payload_indices.write().await;
                payload_indices_guard.insert(req_inner.collection_name.clone(), Arc::new(payload_idx_db));
                info!(collection_name = %req_inner.collection_name, path=?payload_db_path, "PayloadIndexRocksDB initialized successfully for new collection via gRPC.");
            }
            Err(e) => {
                error!(collection_name = %req_inner.collection_name, path=?payload_db_path, error=?e, "CRITICAL: Failed to initialize PayloadIndexRocksDB for new collection via gRPC. Cleaning up.");
                app_state_guard.indices.write().await.remove(&req_inner.collection_name);
                app_state_guard.wal_managers.write().await.remove(&req_inner.collection_name);
                if let Err(cleanup_err) = std::fs::remove_dir_all(data_path_buf.join(&req_inner.collection_name)) {
                    error!(collection_name = %req_inner.collection_name, error=?cleanup_err, "Failed to cleanup collection directory after PayloadIndexRocksDB creation failure.");
                }
                return Err(Status::internal(format!("Failed to initialize payload database: {:?}", e)));
            }
        }

        info!(collection_name = %req_inner.collection_name, "Collection created successfully via gRPC");
        Ok(Response::new(CreateCollectionResponse {}))
    }

    async fn get_collection_info(
        &self,
        request: Request<GetCollectionInfoRequest>,
    ) -> Result<Response<GetCollectionInfoResponse>, Status> {
        let req_inner = request.into_inner();
        info!(collection_name = %req_inner.collection_name, "RPC: GetCollectionInfo received");

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }

        let app_state_guard = self.app_state.read().await;
        let indices_map_guard = app_state_guard.indices.read().await;

        match indices_map_guard.get(&req_inner.collection_name) {
            Some(index_lock_arc) => {
                let index_guard = index_lock_arc.read().await;

                let core_config = index_guard.config();
                let proto_config = core_to_proto_hnsw_config_params(&core_config);
                let proto_metric = core_to_proto_distance_metric(index_guard.distance_metric());

                let vector_count = index_guard.len() as u64;
                let segment_count = index_guard.segment_count() as u64;
                
                let collection_path = app_state_guard.data_path.join(&req_inner.collection_name);
                let disk_size_bytes = fs_extra::dir::get_size(&collection_path).unwrap_or(0);
                
                let estimated_ram_footprint = index_guard.estimate_ram_footprint();
                debug!(ram_bytes = estimated_ram_footprint, "Estimated RAM footprint from index_guard.");
                
                // Basic status check: if index directory and key files exist.
                // A more robust status would involve checking segment health, WAL status, etc.
                let current_status = if collection_path.exists() && 
                                        collection_path.join(format!("{}.hnsw_meta.json", req_inner.collection_name)).exists() {
                    CollectionStatus::Green // Simplified status
                } else {
                    CollectionStatus::Yellow // Or Red if critical files are missing
                };
                tracing::debug!(status = ?current_status, "Determined status for GetCollectionInfoResponse");

                info!(collection_name = %req_inner.collection_name, "Returning collection info");
                Ok(Response::new(GetCollectionInfoResponse {
                    collection_name: req_inner.collection_name.clone(),
                    status: current_status.into(),
                    vector_count,
                    segment_count,
                    disk_size_bytes,
                    ram_footprint_bytes: estimated_ram_footprint,
                    config: Some(proto_config),
                    distance_metric: proto_metric.into(),
                }))
            }
            None => {
                warn!(collection_name = %req_inner.collection_name, "Collection not found for GetCollectionInfo");
                Err(Status::not_found(format!(
                    "Collection '{}' not found",
                    req_inner.collection_name
                )))
            }
        }
    }

    async fn list_collections(
        &self,
        _request: Request<ListCollectionsRequest>, // No params in request for now
    ) -> Result<Response<ListCollectionsResponse>, Status> {
        info!("RPC: ListCollections received");

        let app_state_guard = self.app_state.read().await;
        let indices_map_guard = app_state_guard.indices.read().await;

        let mut collection_descriptions = Vec::new();

        for (name, index_lock_arc) in indices_map_guard.iter() {
            let index_guard = index_lock_arc.read().await;
            let core_config = index_guard.config();
            let proto_metric = core_to_proto_distance_metric(index_guard.distance_metric());
            
            let collection_path_desc = app_state_guard.data_path.join(name);
            let current_status_desc = if collection_path_desc.exists() &&
                                         collection_path_desc.join(format!("{}.hnsw_meta.json", name)).exists() {
                CollectionStatus::Green
            } else {
                CollectionStatus::Yellow
            };
            tracing::debug!(collection_name = %name, status = ?current_status_desc, "Determined status for ListCollectionsResponse item");

            collection_descriptions.push(CollectionDescription {
                name: name.clone(),
                vector_count: index_guard.len() as u64,
                status: current_status_desc.into(),
                dimensions: core_config.vector_dim,
                distance_metric: proto_metric.into(),
            });
        }
        
        debug!(count = collection_descriptions.len(), "Returning collection list");
        Ok(Response::new(ListCollectionsResponse {
            collections: collection_descriptions,
        }))
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        let req_inner = request.into_inner();
        info!(collection_name = %req_inner.collection_name, "RPC: DeleteCollection received");

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }

        let app_state_guard = self.app_state.read().await;
        
        // Check if index exists before attempting to lock for WAL
        if !app_state_guard.indices.read().await.contains_key(&req_inner.collection_name) {
            warn!(collection_name = %req_inner.collection_name, "Collection not found for deletion");
            return Err(Status::not_found(format!(
                "Collection '{}' not found",
                req_inner.collection_name
            )));
        }

        // WAL: Log DeleteCollection operation
        // This should ideally happen *before* actual deletion, but if WAL fails,
        // we might not proceed with deletion. Or, log after successful in-memory removal
        // and before disk removal, with rollback logic.
        // For simplicity, logging before in-memory removal.
        // The WalRecord::DeleteCollection variant needs to be added to the WalRecord enum definition.
        // Assuming it will be: DeleteCollection { name: String }
        let wal_record = WalRecord::DeleteCollection {
            name: req_inner.collection_name.clone(), // Changed from index_name to name to match potential enum definition
        };
        
        // Get WAL manager and log. This requires a read lock on wal_managers.
        {
            let wal_managers_map_guard = app_state_guard.wal_managers.read().await;
            if let Some(wal_manager) = wal_managers_map_guard.get(&req_inner.collection_name) {
                if let Err(e) = wal_manager.log_operation(&wal_record).await {
                    error!(collection_name = %req_inner.collection_name, error = ?e, "CRITICAL: Failed to log DeleteCollection to WAL. Aborting deletion.");
                    return Err(Status::internal(format!("Failed to log DeleteCollection to WAL: {:?}", e)));
                }
            } else {
                // If WAL manager doesn't exist for some reason, but index does, this is an inconsistent state.
                // Log a warning but proceed with deletion of other components if possible.
                warn!(collection_name = %req_inner.collection_name, "WAL manager not found during DeleteCollection. Index might not have been fully created or state is inconsistent.");
            }
        } // WAL manager lock released

        // Remove from in-memory maps
        let _removed_index = app_state_guard.indices.write().await.remove(&req_inner.collection_name);
        let _removed_wal_manager = app_state_guard.wal_managers.write().await.remove(&req_inner.collection_name);
        let _removed_payload_db = app_state_guard.payload_indices.write().await.remove(&req_inner.collection_name);

        // Drop the HnswIndex, WAL manager, and PayloadIndexRocksDB Arcs explicitly to ensure
        // their Drop impls (if any, especially for RocksDB) are called before directory removal.
        // The removal from HashMaps already drops them if their Arc count becomes zero.

        // Delete data from disk
        let collection_path = app_state_guard.data_path.join(&req_inner.collection_name);
        match tokio::fs::remove_dir_all(&collection_path).await {
            Ok(_) => {
                info!(collection_name = %req_inner.collection_name, path = ?collection_path, "Collection data deleted from disk successfully");
            }
            Err(e) => {
                // This is problematic. In-memory state is gone, WAL entry might be there, but disk data remains.
                error!(collection_name = %req_inner.collection_name, path = ?collection_path, error = ?e, "Failed to delete collection data from disk. Manual cleanup may be required.");
                // Return an error, but the in-memory state is already changed.
                return Err(Status::internal(format!(
                    "Failed to delete collection data from disk: {}. In-memory state removed.",
                    e
                )));
            }
        }

        info!(collection_name = %req_inner.collection_name, "Collection deleted successfully via gRPC");
        Ok(Response::new(DeleteCollectionResponse {}))
    }
}
