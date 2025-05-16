use std::collections::HashMap;
use std::collections::BTreeMap; // Ensure BTreeMap is imported
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn, error};
use prost_types::value::Kind;
// ndarray is ex_coviambedding which handles Awrich hray1 internally

use crate::state::AppState;
use crate::grpc_api::vortex_api_v1::{
    points_service_server::PointsService,
    UpsertPointsRequest, UpsertPointsResponse,
    GetPointsRequest, GetPointsResponse,
    DeletePointsRequest, DeletePointsResponse,
    SearchPointsRequest, SearchPointsResponse, SearchParams, // Changed from SearchRequestParams
    PointStruct, PointOperationStatus, StatusCode,
    Vector as ProtoVector, Payload as ProtoPayload, Filter as ProtoFilter, // Removed PointId
};
use crate::wal::wal_manager::WalRecord;
use vortex_core::{Embedding, Index};

// Helper to convert prost_types::Value to serde_json::Value
fn proto_value_to_serde_json_value(proto_value: prost_types::Value) -> Result<serde_json::Value, Status> {
    match proto_value.kind {
        Some(Kind::NullValue(_)) => Ok(serde_json::Value::Null),
        Some(Kind::NumberValue(n)) => {
            if n.is_nan() || n.is_infinite() {
                return Err(Status::invalid_argument("Numeric value is NaN or Infinity, which is not directly supported in standard JSON."));
            }
            if n.fract() == 0.0 {
                 Ok(serde_json::json!(n as i64))
            } else {
                 Ok(serde_json::json!(n))
            }
        }
        Some(Kind::StringValue(s)) => Ok(serde_json::Value::String(s)),
        Some(Kind::BoolValue(b)) => Ok(serde_json::Value::Bool(b)),
        Some(Kind::StructValue(s)) => {
            let mut map = serde_json::Map::new();
            for (k, v) in s.fields {
                map.insert(k, proto_value_to_serde_json_value(v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Some(Kind::ListValue(l)) => {
            let vec: Result<Vec<serde_json::Value>, Status> = l.values.into_iter().map(proto_value_to_serde_json_value).collect();
            Ok(serde_json::Value::Array(vec?))
        }
        None => Ok(serde_json::Value::Null),
    }
}

// Helper to convert gRPC Payload to serde_json::Value
fn proto_payload_to_serde_json(
    proto_payload: Option<ProtoPayload>,
) -> Result<Option<serde_json::Value>, Status> {
    match proto_payload {
        Some(p) => {
            let mut map = serde_json::Map::new();
            for (key, value_proto) in p.fields {
                 match proto_value_to_serde_json_value(value_proto) {
                    Ok(json_val) => map.insert(key, json_val),
                    Err(e) => return Err(Status::invalid_argument(format!("Invalid value in payload field '{}': {}", key, e))),
                };
            }
            Ok(Some(serde_json::Value::Object(map)))
        }
        None => Ok(None),
    }
}

// Helper to convert serde_json::Value to prost_types::Value
fn serde_json_value_to_proto_value(json_value: serde_json::Value) -> Result<prost_types::Value, Status> {
    match json_value {
        serde_json::Value::Null => Ok(prost_types::Value { kind: Some(Kind::NullValue(0)) }),
        serde_json::Value::Bool(b) => Ok(prost_types::Value { kind: Some(Kind::BoolValue(b)) }),
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                Ok(prost_types::Value { kind: Some(Kind::NumberValue(f)) })
            } else {
                Err(Status::internal("Failed to convert serde_json::Number to f64"))
            }
        }
        serde_json::Value::String(s) => Ok(prost_types::Value { kind: Some(Kind::StringValue(s)) }),
        serde_json::Value::Array(a) => {
            let values: Result<Vec<prost_types::Value>, Status> = a.into_iter().map(serde_json_value_to_proto_value).collect();
            Ok(prost_types::Value { kind: Some(Kind::ListValue(prost_types::ListValue { values: values? })) })
        }
        serde_json::Value::Object(o) => {
            let mut fields = BTreeMap::new(); // Reverted to BTreeMap for prost_types::Struct
            for (k, v) in o {
                fields.insert(k, serde_json_value_to_proto_value(v)?);
            }
            Ok(prost_types::Value { kind: Some(Kind::StructValue(prost_types::Struct { fields })) })
        }
    }
}

// Helper to convert serde_json::Value (Option) to gRPC Payload (Option)
fn serde_json_to_proto_payload(
    json_payload_opt: Option<serde_json::Value>,
) -> Result<Option<ProtoPayload>, Status> {
    match json_payload_opt {
        Some(serde_json::Value::Object(map)) => {
            let mut fields = HashMap::new(); // Changed from BTreeMap
            for (key, value_json) in map {
                match serde_json_value_to_proto_value(value_json) {
                    Ok(proto_val) => fields.insert(key, proto_val),
                    Err(e) => return Err(Status::internal(format!("Failed to convert payload field '{}' to proto: {}", key, e))),
                };
            }
            Ok(Some(ProtoPayload { fields }))
        }
        Some(_) => Err(Status::internal("Payload must be a JSON object if provided".to_string())),
        None => Ok(None),
    }
}

// Helper to convert core Embedding to gRPC Vector
fn core_embedding_to_proto_vector(embedding: &Embedding) -> ProtoVector {
    ProtoVector {
        elements: embedding.to_vec(), // ProtoVector uses 'elements'
    }
}


#[derive(Debug)]
pub struct PointsServerImpl {
    pub app_state: Arc<RwLock<AppState>>,
}

#[tonic::async_trait]
impl PointsService for PointsServerImpl {
    async fn upsert_points(
        &self,
        request: Request<UpsertPointsRequest>,
    ) -> Result<Response<UpsertPointsResponse>, Status> {
        let req_inner = request.into_inner();
        info!(
            collection_name = %req_inner.collection_name,
            num_points = req_inner.points.len(),
            wait_flush = req_inner.wait_flush.unwrap_or(false),
            "RPC: UpsertPoints received"
        );
        if req_inner.wait_flush.unwrap_or(false) {
            // TODO: Implement actual wait_flush logic in WAL and call it here.
            debug!("wait_flush=true is requested but not yet fully implemented for UpsertPoints.");
        }

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }
        if req_inner.points.is_empty() {
            return Ok(Response::new(UpsertPointsResponse {
                statuses: vec![],
                overall_error: None,
            }));
        }

        let app_state_guard = self.app_state.read().await;

        let index_lock_arc = {
            let indices_map_guard = app_state_guard.indices.read().await;
            indices_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Collection '{}' not found", req_inner.collection_name)))?
        };
        
        let wal_manager_arc = {
            let wal_managers_map_guard = app_state_guard.wal_managers.read().await;
            wal_managers_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::internal(format!("WAL manager not found for collection '{}'", req_inner.collection_name)))?
        };

        let payload_db_arc = {
            let payload_indices_guard = app_state_guard.payload_indices.read().await;
            payload_indices_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::internal(format!("Payload database not found for collection '{}'", req_inner.collection_name)))?
        };
        
        let mut statuses = Vec::with_capacity(req_inner.points.len());
        let mut overall_error_message: Option<String> = None;

        let expected_dims = {
            let index_guard = index_lock_arc.read().await;
            index_guard.dimensions()
        };

        let mut validated_points = Vec::with_capacity(req_inner.points.len());
        for point_proto in req_inner.points { // point_proto is PointStruct
            let point_id_val = point_proto.id.clone(); // Changed: PointStruct.id is String
            if point_id_val.is_empty() {
                let err_msg = "Point ID cannot be empty".to_string();
                statuses.push(PointOperationStatus {
                    point_id: point_id_val.clone(),
                    status_code: StatusCode::InvalidArgument.into(),
                    error_message: Some(err_msg.clone()),
                });
                if overall_error_message.is_none() { overall_error_message = Some(err_msg); }
                continue;
            }
            match &point_proto.vector {
                Some(v_proto) => {
                    if v_proto.elements.len() != expected_dims { 
                        let err_msg = format!(
                            "Dimension mismatch for point ID '{}': expected {}, got {}",
                            point_id_val, expected_dims, v_proto.elements.len()
                        );
                        statuses.push(PointOperationStatus {
                            point_id: point_id_val,
                            status_code: StatusCode::InvalidArgument.into(),
                            error_message: Some(err_msg.clone()),
                        });
                        if overall_error_message.is_none() { overall_error_message = Some(err_msg); }
                        continue;
                    }
                }
                None => {
                    let err_msg = format!("Vector is missing for point ID '{}'", point_id_val);
                    statuses.push(PointOperationStatus {
                        point_id: point_id_val,
                        status_code: StatusCode::InvalidArgument.into(),
                        error_message: Some(err_msg.clone()),
                    });
                    if overall_error_message.is_none() { overall_error_message = Some(err_msg); }
                    continue;
                }
            }
            validated_points.push(point_proto);
        }
        
        if overall_error_message.is_some() {
             return Ok(Response::new(UpsertPointsResponse { statuses, overall_error: overall_error_message }));
        }

        let mut index_guard = index_lock_arc.write().await;

        for point_proto in validated_points { // point_proto is PointStruct
            let point_id_str = point_proto.id.clone(); // Changed: PointStruct.id is String
            let vector_elements = point_proto.vector.unwrap().elements; 
            let core_embedding = Embedding::from(vector_elements); 
            
            let core_payload_opt = match proto_payload_to_serde_json(point_proto.payload) {
                Ok(p) => p,
                Err(e) => {
                    statuses.push(PointOperationStatus {
                        point_id: point_id_str.clone(),
                        status_code: StatusCode::InvalidArgument.into(),
                        error_message: Some(format!("Failed to parse payload: {}", e)),
                    });
                    if overall_error_message.is_none() { overall_error_message = Some("Error processing one or more point payloads.".to_string()); }
                    continue;
                }
            };
            
            let wal_record = WalRecord::AddVector {
                vector_id: point_id_str.clone(),
                vector: core_embedding.clone(),
                metadata: core_payload_opt.clone(),
            };

            if let Err(e) = wal_manager_arc.log_operation(&wal_record).await {
                error!(collection_name = %req_inner.collection_name, point_id = %point_id_str, error = ?e, "Failed to log AddVector to WAL");
                statuses.push(PointOperationStatus {
                    point_id: point_id_str,
                    status_code: StatusCode::Error.into(),
                    error_message: Some(format!("WAL error: {}", e)),
                });
                if overall_error_message.is_none() { overall_error_message = Some("One or more points failed due to WAL error.".to_string()); }
                continue;
            }

            match index_guard.add_vector(point_id_str.clone(), core_embedding).await {
                Ok(_added) => {
                    if let Some(payload_val) = core_payload_opt {
                        if let Err(e_payload) = payload_db_arc.set_payload(&point_id_str, &payload_val) {
                            warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str, error = ?e_payload, "Failed to set payload in RocksDB. Vector added to HNSW, but payload failed.");
                            statuses.push(PointOperationStatus {
                                point_id: point_id_str,
                                status_code: StatusCode::Ok.into(),
                                error_message: Some("Vector upserted, but payload update failed.".to_string()),
                            });
                        } else {
                             statuses.push(PointOperationStatus {
                                point_id: point_id_str,
                                status_code: StatusCode::Ok.into(),
                                error_message: None,
                            });
                        }
                    } else {
                        statuses.push(PointOperationStatus {
                            point_id: point_id_str,
                            status_code: StatusCode::Ok.into(),
                            error_message: None,
                        });
                    }
                }
                Err(e) => {
                    error!(collection_name = %req_inner.collection_name, point_id = %point_id_str, error = ?e, "Failed to add vector to HNSW index");
                    statuses.push(PointOperationStatus {
                        point_id: point_id_str,
                        status_code: StatusCode::Error.into(),
                        error_message: Some(format!("HNSW add error: {}", e)),
                    });
                    if overall_error_message.is_none() { overall_error_message = Some("One or more points failed during HNSW add.".to_string()); }
                }
            }
        }
        
        info!(collection_name = %req_inner.collection_name, num_statuses = statuses.len(), "RPC: UpsertPoints completed");
        Ok(Response::new(UpsertPointsResponse { statuses, overall_error: overall_error_message }))
    }

    async fn get_points(
        &self,
        request: Request<GetPointsRequest>,
    ) -> Result<Response<GetPointsResponse>, Status> {
        let req_inner = request.into_inner();
        info!(
            collection_name = %req_inner.collection_name,
            num_ids = req_inner.ids.len(),
            with_payload = req_inner.with_payload.unwrap_or(true),
            with_vector = req_inner.with_vector.unwrap_or(false),
            "RPC: GetPoints received"
        );

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }
        if req_inner.ids.is_empty() {
            return Ok(Response::new(GetPointsResponse { points: vec![] }));
        }

        let app_state_guard = self.app_state.read().await;
        let index_lock_arc = {
            let indices_map_guard = app_state_guard.indices.read().await;
            indices_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Collection '{}' not found", req_inner.collection_name)))?
        };
        let payload_db_arc_opt = {
            let payload_indices_guard = app_state_guard.payload_indices.read().await;
            payload_indices_guard.get(&req_inner.collection_name).cloned()
        };

        let index_guard = index_lock_arc.read().await;

        let mut result_points = Vec::new();
        let include_payload = req_inner.with_payload.unwrap_or(true);
        let include_vector = req_inner.with_vector.unwrap_or(false);

        for point_id_str_val_loop in req_inner.ids { // point_id_str_val_loop is String
            let point_id_str_val = &point_id_str_val_loop; // Changed: point_id_str_val_loop is String, use its reference
            if point_id_str_val.is_empty() {
                warn!(collection_name = %req_inner.collection_name, "Empty point ID requested in GetPoints");
                continue;
            }
            
            let mut proto_vector: Option<ProtoVector> = None;
            let mut proto_payload: Option<ProtoPayload> = None;
            let mut point_exists = false;

            if include_vector {
                match index_guard.get_vector(point_id_str_val).await {
                    Ok(Some(embedding)) => {
                        proto_vector = Some(core_embedding_to_proto_vector(&embedding));
                        point_exists = true;
                    }
                    Ok(None) => { /* Point not found */ }
                    Err(e) => {
                        error!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Error getting vector from HNSW");
                        continue;
                    }
                }
            } else {
                 match index_guard.get_vector(point_id_str_val).await { 
                    Ok(Some(_)) => point_exists = true,
                    Ok(None) => point_exists = false,
                    Err(e) => {
                        error!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Error checking point existence");
                        continue;
                    }
                }
            }
            
            if include_payload && point_exists {
                if let Some(payload_db) = &payload_db_arc_opt {
                    match payload_db.get_payload(point_id_str_val) {
                        Ok(Some(json_val)) => {
                            match serde_json_to_proto_payload(Some(json_val)) {
                                Ok(p) => proto_payload = p,
                                Err(e) => {
                                     warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Failed to convert payload to proto for GetPoints");
                                }
                            }
                        }
                        Ok(None) => { /* No payload */ }
                        Err(e) => {
                            warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Error getting payload from RocksDB for GetPoints");
                        }
                    }
                } else {
                     warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, "Payload DB not found for GetPoints, cannot retrieve payload.");
                }
            }
            
            if point_exists { 
                 result_points.push(PointStruct {
                    id: point_id_str_val_loop.clone(), // Changed: PointStruct.id is String
                    vector: proto_vector,
                    payload: proto_payload,
                });
            }
        }

        info!(collection_name = %req_inner.collection_name, num_retrieved = result_points.len(), "RPC: GetPoints completed");
        Ok(Response::new(GetPointsResponse { points: result_points }))
    }

    async fn delete_points(
        &self,
        request: Request<DeletePointsRequest>,
    ) -> Result<Response<DeletePointsResponse>, Status> {
        let req_inner = request.into_inner();
        info!(
            collection_name = %req_inner.collection_name,
            num_ids = req_inner.ids.len(),
            wait_flush = req_inner.wait_flush.unwrap_or(false),
            "RPC: DeletePoints received"
        );
        if req_inner.wait_flush.unwrap_or(false) {
            // TODO: Implement actual wait_flush logic in WAL and call it here.
            debug!("wait_flush=true is requested but not yet fully implemented for DeletePoints.");
        }

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }
        if req_inner.ids.is_empty() {
            return Ok(Response::new(DeletePointsResponse {
                statuses: vec![],
                overall_error: Some("No point IDs provided for deletion.".to_string()),
            }));
        }

        let app_state_guard = self.app_state.read().await;

        let index_lock_arc = {
            let indices_map_guard = app_state_guard.indices.read().await;
            indices_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Collection '{}' not found", req_inner.collection_name)))?
        };
        
        let wal_manager_arc = {
            let wal_managers_map_guard = app_state_guard.wal_managers.read().await;
            wal_managers_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::internal(format!("WAL manager not found for collection '{}'", req_inner.collection_name)))?
        };

        let payload_db_arc = {
            let payload_indices_guard = app_state_guard.payload_indices.read().await;
            payload_indices_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::internal(format!("Payload database not found for collection '{}'", req_inner.collection_name)))?
        };

        let mut statuses = Vec::with_capacity(req_inner.ids.len());
        let mut overall_error_message: Option<String> = None;
        
        let mut index_guard = index_lock_arc.write().await;

        for point_id_str_val_loop in req_inner.ids { // point_id_str_val_loop is String
            let point_id_str_val = &point_id_str_val_loop; // Changed: point_id_str_val_loop is String, use its reference
            if point_id_str_val.is_empty() {
                statuses.push(PointOperationStatus {
                    point_id: "".to_string(),
                    status_code: StatusCode::InvalidArgument.into(),
                    error_message: Some("Point ID cannot be empty".to_string()),
                });
                if overall_error_message.is_none() { overall_error_message = Some("One or more point IDs are invalid.".to_string()); }
                continue;
            }

            let wal_record = WalRecord::DeleteVector {
                vector_id: point_id_str_val.clone(),
            };

            if let Err(e) = wal_manager_arc.log_operation(&wal_record).await {
                error!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Failed to log DeleteVector to WAL");
                statuses.push(PointOperationStatus {
                    point_id: point_id_str_val.clone(),
                    status_code: StatusCode::Error.into(),
                    error_message: Some(format!("WAL error: {}", e)),
                });
                if overall_error_message.is_none() { overall_error_message = Some("One or more points failed due to WAL error.".to_string()); }
                continue;
            }

            match index_guard.delete_vector(point_id_str_val).await {
                Ok(deleted) => {
                    if deleted {
                        if let Err(e_payload) = payload_db_arc.delete_payload(point_id_str_val) {
                            warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e_payload, "Failed to delete payload from RocksDB. Vector deleted from HNSW, but payload deletion failed.");
                            statuses.push(PointOperationStatus {
                                point_id: point_id_str_val.clone(),
                                status_code: StatusCode::Ok.into(),
                                error_message: Some("Vector deleted, but payload deletion failed.".to_string()),
                            });
                        } else {
                            statuses.push(PointOperationStatus {
                                point_id: point_id_str_val.clone(),
                                status_code: StatusCode::Ok.into(),
                                error_message: None,
                            });
                        }
                    } else {
                        statuses.push(PointOperationStatus {
                            point_id: point_id_str_val.clone(),
                            status_code: StatusCode::NotFound.into(),
                            error_message: Some("Point not found in HNSW index".to_string()),
                        });
                    }
                }
                Err(e) => {
                    error!(collection_name = %req_inner.collection_name, point_id = %point_id_str_val, error = ?e, "Failed to delete vector from HNSW index");
                    statuses.push(PointOperationStatus {
                        point_id: point_id_str_val.clone(),
                        status_code: StatusCode::Error.into(),
                        error_message: Some(format!("HNSW delete error: {}", e)),
                    });
                    if overall_error_message.is_none() { overall_error_message = Some("One or more points failed during HNSW delete.".to_string()); }
                }
            }
        }
        info!(collection_name = %req_inner.collection_name, num_statuses = statuses.len(), "RPC: DeletePoints completed");
        Ok(Response::new(DeletePointsResponse { statuses, overall_error: overall_error_message }))
    }

    async fn search_points(
        &self,
        request: Request<SearchPointsRequest>,
    ) -> Result<Response<SearchPointsResponse>, Status> {
        let req_inner = request.into_inner();
        info!(
            collection_name = %req_inner.collection_name,
            k_limit = req_inner.k_limit, 
            has_filter = req_inner.filter.is_some(),
            with_payload = req_inner.with_payload.unwrap_or(true),
            with_vector = req_inner.with_vector.unwrap_or(false),
            "RPC: SearchPoints received"
        );

        if req_inner.collection_name.is_empty() {
            return Err(Status::invalid_argument("Collection name cannot be empty"));
        }
        let query_proto_vector = req_inner.query_vector.ok_or_else(|| Status::invalid_argument("Query vector is missing"))?;
        if query_proto_vector.elements.is_empty() { 
            return Err(Status::invalid_argument("Query vector cannot be empty"));
        }
        if req_inner.k_limit == 0 {
            return Ok(Response::new(SearchPointsResponse { results: vec![] }));
        }

        let app_state_guard = self.app_state.read().await;
        let index_lock_arc = {
            let indices_map_guard = app_state_guard.indices.read().await;
            indices_map_guard
                .get(&req_inner.collection_name)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("Collection '{}' not found", req_inner.collection_name)))?
        };
        let payload_db_arc_opt = {
            let payload_indices_guard = app_state_guard.payload_indices.read().await;
            payload_indices_guard.get(&req_inner.collection_name).cloned()
        };

        let index_guard = index_lock_arc.read().await;
        let core_embedding = Embedding::from(query_proto_vector.elements); 

        if core_embedding.len() != index_guard.dimensions() {
            return Err(Status::invalid_argument(format!(
                "Query vector dimension mismatch: expected {}, got {}",
                index_guard.dimensions(),
                core_embedding.len()
            )));
        }

        let k = req_inner.k_limit as usize;
        let include_payload = req_inner.with_payload.unwrap_or(true);
        let include_vector = req_inner.with_vector.unwrap_or(false);
        
        let serde_filter_opt: Option<serde_json::Value> = match req_inner.filter {
            Some(ProtoFilter { must_match_exact }) => {
                let mut filter_map = serde_json::Map::new();
                for (key, value_proto) in must_match_exact {
                    match proto_value_to_serde_json_value(value_proto) {
                        Ok(json_val) => filter_map.insert(key, json_val),
                        Err(e) => return Err(Status::invalid_argument(format!("Invalid value in filter: {}", e))),
                    };
                }
                if filter_map.is_empty() { None } else { Some(serde_json::Value::Object(filter_map)) }
            }
            None => None,
        };
        
        let k_to_fetch = if serde_filter_opt.is_some() {
            std::cmp::max(k * 5, k + 50).min(1000)
        } else {
            k
        };
        
        let hnsw_ef_search = req_inner.params.as_ref()
            .and_then(|p: &SearchParams| p.ef_search) // Changed: SearchParams
            .map(|ef| ef as usize)
            .unwrap_or_else(|| index_guard.config().ef_search);
        let final_ef_search = std::cmp::max(k_to_fetch, hnsw_ef_search);
        
        let initial_search_results = match index_guard.search_with_ef(core_embedding, k_to_fetch, final_ef_search).await {
            Ok(res) => res,
            Err(e) => {
                error!(collection_name = %req_inner.collection_name, error = ?e, "HNSW search failed");
                return Err(Status::internal(format!("Search failed: {}", e)));
            }
        };
        debug!(collection_name = %req_inner.collection_name, initial_count = initial_search_results.len(), "Initial search completed");

        let mut final_proto_results = Vec::with_capacity(k);

        for (id_val, score_val) in &initial_search_results {
            if final_proto_results.len() >= k {
                break;
            }

            let point_id_str = id_val.clone();
            let score = *score_val;
            let mut result_proto_payload: Option<ProtoPayload> = None;
            let mut result_proto_vector: Option<ProtoVector> = None;

            let mut passes_filter = true;

            if let Some(serde_filter_val) = &serde_filter_opt {
                if let Some(payload_db) = &payload_db_arc_opt {
                    match payload_db.get_payload(&point_id_str) {
                        Ok(Some(actual_payload_json)) => {
                            if let (Some(filter_obj_map), Some(payload_obj_map)) = (serde_filter_val.as_object(), actual_payload_json.as_object()) {
                                if !filter_obj_map.is_empty() {
                                    passes_filter = crate::handlers::matches_filter(payload_obj_map, filter_obj_map);
                                }
                            } else { 
                                passes_filter = serde_filter_val.as_object().map_or(true, |obj| obj.is_empty());
                            }
                            if passes_filter && include_payload {
                                result_proto_payload = serde_json_to_proto_payload(Some(actual_payload_json)).unwrap_or(None);
                            }
                        }
                        Ok(None) => { 
                            passes_filter = serde_filter_val.as_object().map_or(true, |obj| obj.is_empty());
                        }
                        Err(e) => {
                            warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str, error = ?e, "Failed to get payload from RocksDB for search result. Assuming filter mismatch.");
                            passes_filter = false;
                        }
                    }
                } else { 
                    warn!(collection_name = %req_inner.collection_name, "Filter requested but PayloadIndexRocksDB not found. Assuming filter mismatch for all points.");
                    passes_filter = serde_filter_val.as_object().map_or(true, |obj| obj.is_empty());
                }
            } else if include_payload { 
                 if let Some(payload_db) = &payload_db_arc_opt {
                    if let Ok(Some(json_val)) = payload_db.get_payload(&point_id_str) {
                        result_proto_payload = serde_json_to_proto_payload(Some(json_val)).unwrap_or(None);
                    }
                 }
            }

            // TODO: Optimize payload fetching if filter is active and include_payload is true to avoid double fetch.
            if passes_filter {
                if include_vector {
                    match index_guard.get_vector(&point_id_str).await {
                        Ok(Some(embedding)) => {
                            result_proto_vector = Some(core_embedding_to_proto_vector(&embedding));
                        }
                        Ok(None) => { /* Should not happen if it was found in HNSW search */ }
                        Err(e) => {
                             warn!(collection_name = %req_inner.collection_name, point_id = %point_id_str, error = ?e, "Failed to re-fetch vector for search result.");
                        }
                    }
                }
                // TODO: Implement actual versioning for points.
                let current_version = Some(0); 
                debug!(point_id = %point_id_str, "Placeholder value used for version in ScoredPoint");

                final_proto_results.push(crate::grpc_api::vortex_api_v1::ScoredPoint {
                    id: point_id_str, 
                    payload: result_proto_payload,
                    vector: result_proto_vector,
                    score,
                    version: current_version,
                });
            }
        }
        
        info!(collection_name = %req_inner.collection_name, num_results = final_proto_results.len(), "RPC: SearchPoints completed");
        Ok(Response::new(SearchPointsResponse { results: final_proto_results }))
    }
}
