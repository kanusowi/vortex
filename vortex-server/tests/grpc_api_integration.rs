use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::sleep;
use tonic::transport::Channel;
use tracing::info;
use tokio_stream::wrappers::TcpListenerStream;

use vortex_server::state::AppState;
use vortex_server::grpc_api::vortex_api_v1::collections_service_client::CollectionsServiceClient;
use vortex_server::grpc_api::vortex_api_v1::points_service_client::PointsServiceClient;
use vortex_server::grpc_api::vortex_api_v1::{
    CreateCollectionRequest, DeleteCollectionRequest, DistanceMetric,
    GetCollectionInfoRequest, HnswConfigParams, ListCollectionsRequest, PointStruct,
    SearchPointsRequest, UpsertPointsRequest, Vector, GetPointsRequest,
    DeletePointsRequest as GrpcDeletePointsRequest, // Renamed to avoid conflict
    /*ScoredPoint, Filter,*/ SearchParams, Payload, // Commented out unused Filter, ScoredPoint
    // CollectionExistsRequest is not a message, this check is done via GetCollectionInfo
};
use vortex_server::grpc_services::{CollectionsServerImpl, PointsServerImpl};

async fn setup_test_server() -> Result<
    (
        CollectionsServiceClient<Channel>,
        PointsServiceClient<Channel>,
        SocketAddr,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ),
    Box<dyn std::error::Error>,
> {
    let data_path = tempfile::tempdir()?.path().to_path_buf();
    info!("Test server data path: {:?}", data_path);

    let app_state_instance = AppState::new(data_path.clone()); // Direct assignment
    let app_state = Arc::new(RwLock::new(app_state_instance));

    let app_state_for_server = app_state.clone();

    let collections_service_impl =
        CollectionsServerImpl { app_state: app_state_for_server.clone() };
    let points_service_impl =
        PointsServerImpl { app_state: app_state_for_server };

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;
    info!("gRPC server listening on: {}", actual_addr);

    let listener_stream = TcpListenerStream::new(listener);

    let server_handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                vortex_server::grpc_api::vortex_api_v1::collections_service_server::CollectionsServiceServer::new(
                    collections_service_impl,
                ),
            )
            .add_service(
                vortex_server::grpc_api::vortex_api_v1::points_service_server::PointsServiceServer::new(
                    points_service_impl,
                ),
            )
            .serve_with_incoming_shutdown(listener_stream, async {
                tokio::signal::ctrl_c().await.unwrap();
                info!("gRPC server shutting down");
            })
            .await
    });

    sleep(Duration::from_millis(200)).await;

    let endpoint_uri = format!("http://{}", actual_addr);
    let collections_client = CollectionsServiceClient::connect(endpoint_uri.clone()).await?;
    let points_client = PointsServiceClient::connect(endpoint_uri).await?;

    Ok((
        collections_client,
        points_client,
        actual_addr,
        server_handle,
    ))
}

#[tokio::test]
async fn test_grpc_server_starts_and_basic_ping() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, _points_client, _addr, server_handle) =
        setup_test_server().await?;

    let request = tonic::Request::new(ListCollectionsRequest {});
    let response = collections_client.list_collections(request).await?;
    assert!(response.into_inner().collections.is_empty());

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_points_service_error_cases() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_ok = "points_errors_collection_ok".to_string();
    let collection_name_non_existent = "points_errors_collection_non_existent".to_string();
    let vector_dims = 3;

    // Setup a valid collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_ok.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 16, ef_construction: 100, ef_search: 50, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 32,
        }),
    })).await?;

    // --- UpsertPoints Error Cases ---
    // 1. Upsert to a non-existent collection
    let upsert_non_existent_coll_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_non_existent.clone(),
        points: vec![PointStruct {
            id: "p1".to_string(),
            vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
            payload: None,
        }],
        wait_flush: None,
    });
    let result_upsert_non_existent_coll = points_client.upsert_points(upsert_non_existent_coll_req).await;
    assert!(result_upsert_non_existent_coll.is_err());
    if let Err(status) = result_upsert_non_existent_coll {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 2. Upsert with empty collection name
    let upsert_empty_coll_name_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: "".to_string(),
        points: vec![PointStruct {
            id: "p1".to_string(),
            vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
            payload: None,
        }],
        wait_flush: None,
    });
    let result_upsert_empty_coll_name = points_client.upsert_points(upsert_empty_coll_name_req).await;
    assert!(result_upsert_empty_coll_name.is_err());
    if let Err(status) = result_upsert_empty_coll_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Collection name cannot be empty"));
    }
    
    // 3. Upsert with empty points list (should be Ok, but with empty statuses)
    let upsert_empty_points_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_ok.clone(),
        points: vec![], // Empty points
        wait_flush: None,
    });
    let response_upsert_empty_points = points_client.upsert_points(upsert_empty_points_req).await?.into_inner();
    assert!(response_upsert_empty_points.statuses.is_empty());
    assert!(response_upsert_empty_points.overall_error.is_none());


    // 4. Upsert batch with some invalid points (dimension mismatch, empty ID)
    let points_mixed_validity = vec![
        PointStruct { // Valid
            id: "valid_p1".to_string(),
            vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
            payload: None,
        },
        PointStruct { // Invalid: dimension mismatch
            id: "invalid_dim_p2".to_string(),
            vector: Some(Vector { elements: vec![1.0, 2.0] }), // Expected 3 dims
            payload: None,
        },
        PointStruct { // Invalid: empty ID
            id: "".to_string(),
            vector: Some(Vector { elements: vec![4.0, 5.0, 6.0] }),
            payload: None,
        },
        PointStruct { // Valid
            id: "valid_p3".to_string(),
            vector: Some(Vector { elements: vec![7.0, 8.0, 9.0] }),
            payload: None,
        },
        PointStruct { // Invalid: missing vector
            id: "invalid_missing_vec_p4".to_string(),
            vector: None,
            payload: None,
        },
    ];
    let upsert_mixed_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_ok.clone(),
        points: points_mixed_validity,
        wait_flush: None,
    });
    let response_upsert_mixed = points_client.upsert_points(upsert_mixed_req).await?.into_inner();
    assert_eq!(response_upsert_mixed.statuses.len(), 3); // 3 invalid points lead to 3 statuses in the pre-validation phase
    assert!(response_upsert_mixed.overall_error.is_some());

    let status_dim_mismatch = response_upsert_mixed.statuses.iter().find(|s| s.point_id == "invalid_dim_p2").unwrap();
    assert_eq!(status_dim_mismatch.status_code, vortex_server::grpc_api::vortex_api_v1::StatusCode::InvalidArgument as i32);
    assert!(status_dim_mismatch.error_message.as_ref().unwrap().contains("Dimension mismatch"));
    
    let status_empty_id = response_upsert_mixed.statuses.iter().find(|s| s.point_id == "").unwrap();
    assert_eq!(status_empty_id.status_code, vortex_server::grpc_api::vortex_api_v1::StatusCode::InvalidArgument as i32);
    assert!(status_empty_id.error_message.as_ref().unwrap().contains("Point ID cannot be empty"));

    let status_missing_vec = response_upsert_mixed.statuses.iter().find(|s| s.point_id == "invalid_missing_vec_p4").unwrap();
    assert_eq!(status_missing_vec.status_code, vortex_server::grpc_api::vortex_api_v1::StatusCode::InvalidArgument as i32);
    assert!(status_missing_vec.error_message.as_ref().unwrap().contains("Vector is missing"));

    // Check that the valid points were NOT inserted because overall_error was set due to pre-validation failures
    let get_valid_points_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_ok.clone(),
        ids: vec!["valid_p1".to_string(), "valid_p3".to_string()],
        with_payload: Some(false),
        with_vector: Some(false),
    });
    let response_get_valid_points = points_client.get_points(get_valid_points_req).await?.into_inner();
    assert!(response_get_valid_points.points.is_empty(), "Valid points should not have been inserted due to batch pre-validation failure.");

    // --- GetPoints Error Cases ---
    // 5. GetPoints from a non-existent collection
    let get_non_existent_coll_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_non_existent.clone(),
        ids: vec!["p1".to_string()],
        with_payload: None, with_vector: None,
    });
    let result_get_non_existent_coll = points_client.get_points(get_non_existent_coll_req).await;
    assert!(result_get_non_existent_coll.is_err());
    if let Err(status) = result_get_non_existent_coll {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 6. GetPoints with empty collection name
    let get_empty_coll_name_req = tonic::Request::new(GetPointsRequest {
        collection_name: "".to_string(),
        ids: vec!["p1".to_string()],
        with_payload: None, with_vector: None,
    });
    let result_get_empty_coll_name = points_client.get_points(get_empty_coll_name_req).await;
    assert!(result_get_empty_coll_name.is_err());
    if let Err(status) = result_get_empty_coll_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // 7. GetPoints with empty IDs list (should be Ok, empty points)
    let get_empty_ids_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_ok.clone(),
        ids: vec![],
        with_payload: None, with_vector: None,
    });
    let response_get_empty_ids = points_client.get_points(get_empty_ids_req).await?.into_inner();
    assert!(response_get_empty_ids.points.is_empty());

    // --- DeletePoints Error Cases ---
    // 8. DeletePoints from a non-existent collection
    let delete_non_existent_coll_req = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: collection_name_non_existent.clone(),
        ids: vec!["p1".to_string()],
        wait_flush: None,
    });
    let result_delete_non_existent_coll = points_client.delete_points(delete_non_existent_coll_req).await;
    assert!(result_delete_non_existent_coll.is_err());
    if let Err(status) = result_delete_non_existent_coll {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 9. DeletePoints with empty collection name
    let delete_empty_coll_name_req = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: "".to_string(),
        ids: vec!["p1".to_string()],
        wait_flush: None,
    });
    let result_delete_empty_coll_name = points_client.delete_points(delete_empty_coll_name_req).await;
    assert!(result_delete_empty_coll_name.is_err());
    if let Err(status) = result_delete_empty_coll_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // 10. DeletePoints with empty IDs list (should be Ok, specific error message in response)
    let delete_empty_ids_req = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: collection_name_ok.clone(),
        ids: vec![],
        wait_flush: None,
    });
    let response_delete_empty_ids = points_client.delete_points(delete_empty_ids_req).await?.into_inner();
    assert!(response_delete_empty_ids.statuses.is_empty());
    assert!(response_delete_empty_ids.overall_error.unwrap().contains("No point IDs provided"));


    // --- SearchPoints Error Cases ---
    // 11. Search in a non-existent collection
    let search_non_existent_coll_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_non_existent.clone(),
        query_vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
        k_limit: 1, filter: None, with_payload: None, with_vector: None, params: None,
    });
    let result_search_non_existent_coll = points_client.search_points(search_non_existent_coll_req).await;
    assert!(result_search_non_existent_coll.is_err());
    if let Err(status) = result_search_non_existent_coll {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 12. Search with empty collection name
    let search_empty_coll_name_req = tonic::Request::new(SearchPointsRequest {
        collection_name: "".to_string(),
        query_vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
        k_limit: 1, filter: None, with_payload: None, with_vector: None, params: None,
    });
    let result_search_empty_coll_name = points_client.search_points(search_empty_coll_name_req).await;
    assert!(result_search_empty_coll_name.is_err());
    if let Err(status) = result_search_empty_coll_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // 13. Search with missing query vector
    let search_missing_query_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_ok.clone(),
        query_vector: None, // Missing
        k_limit: 1, filter: None, with_payload: None, with_vector: None, params: None,
    });
    let result_search_missing_query = points_client.search_points(search_missing_query_req).await;
    assert!(result_search_missing_query.is_err());
    if let Err(status) = result_search_missing_query {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Query vector is missing"));
    }
    
    // 14. Search with empty query vector
    let search_empty_query_vec_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_ok.clone(),
        query_vector: Some(Vector { elements: vec![] }), // Empty
        k_limit: 1, filter: None, with_payload: None, with_vector: None, params: None,
    });
    let result_search_empty_query_vec = points_client.search_points(search_empty_query_vec_req).await;
    assert!(result_search_empty_query_vec.is_err());
    if let Err(status) = result_search_empty_query_vec {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Query vector cannot be empty"));
    }

    // 15. Search with k_limit = 0 (should be Ok, empty results)
    let search_k0_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_ok.clone(),
        query_vector: Some(Vector { elements: vec![1.0, 2.0, 3.0] }),
        k_limit: 0, // k=0
        filter: None, with_payload: None, with_vector: None, params: None,
    });
    let response_search_k0 = points_client.search_points(search_k0_req).await?.into_inner();
    assert!(response_search_k0.results.is_empty());

    // 16. Search with query vector dimension mismatch
    let search_dim_mismatch_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_ok.clone(),
        query_vector: Some(Vector { elements: vec![1.0, 2.0] }), // 2 dims, expected 3
        k_limit: 1, filter: None, with_payload: None, with_vector: None, params: None,
    });
    let result_search_dim_mismatch = points_client.search_points(search_dim_mismatch_req).await;
    assert!(result_search_dim_mismatch.is_err());
    if let Err(status) = result_search_dim_mismatch {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Query vector dimension mismatch"));
    }

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest{
        collection_name: collection_name_ok.clone(),
    })).await?;

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_collections_service_error_cases() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, _points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_str = "test_collection_errors".to_string();
    let vector_dims = 3;

    // 1. Test creating a collection that already exists
    let create_request_1 = tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 16, ef_construction: 100, ef_search: 50, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 32,
        }),
    });
    collections_client.create_collection(create_request_1).await?;

    let create_request_2 = tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(), // Same name
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 16, ef_construction: 100, ef_search: 50, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 32,
        }),
    });
    let result_already_exists = collections_client.create_collection(create_request_2).await;
    assert!(result_already_exists.is_err());
    if let Err(status) = result_already_exists {
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
    }

    // 2. Test invalid HNSW parameters (m=0)
    let create_invalid_m_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "test_invalid_m".to_string(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 0, ef_construction: 100, ef_search: 50, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 32,
        }),
    });
    let result_invalid_m = collections_client.create_collection(create_invalid_m_request).await;
    assert!(result_invalid_m.is_err());
    if let Err(status) = result_invalid_m {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("M must be greater than 0")); // Corrected capitalization
    }
    
    // 3. Test invalid HNSW parameters (ef_construction=0)
    let create_invalid_efc_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "test_invalid_efc".to_string(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 16, ef_construction: 0, ef_search: 50, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 32,
        }),
    });
    let result_invalid_efc = collections_client.create_collection(create_invalid_efc_request).await;
    assert!(result_invalid_efc.is_err());
    if let Err(status) = result_invalid_efc {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("ef_construction must be greater than 0")); // Corrected message
    }

    // 4. Test invalid vector dimensions (0)
    let create_invalid_dims_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "test_invalid_dims".to_string(),
        vector_dimensions: 0, // Invalid
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: None,
    });
    let result_invalid_dims = collections_client.create_collection(create_invalid_dims_request).await;
    assert!(result_invalid_dims.is_err());
    if let Err(status) = result_invalid_dims {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Vector dimensions must be greater than 0"));
    }
    
    // 5. Test unspecified distance metric
    let create_unspec_metric_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "test_unspec_metric".to_string(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Unspecified as i32, // Invalid
        hnsw_config: None,
    });
    let result_unspec_metric = collections_client.create_collection(create_unspec_metric_request).await;
    assert!(result_unspec_metric.is_err());
    if let Err(status) = result_unspec_metric {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Distance metric cannot be unspecified"));
    }

    // 6. Test empty collection name
    let create_empty_name_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "".to_string(), // Invalid
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: None,
    });
    let result_empty_name = collections_client.create_collection(create_empty_name_request).await;
    assert!(result_empty_name.is_err());
    if let Err(status) = result_empty_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Collection name cannot be empty"));
    }

    // 7. Test HNSW config vector_dim mismatch with request vector_dimensions
    let create_dim_mismatch_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: "test_dim_mismatch".to_string(),
        vector_dimensions: vector_dims, // e.g. 3
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 16, ef_construction: 100, ef_search: 50, ml: 0.3, seed: None, 
            vector_dim: vector_dims + 1, // Mismatch, e.g. 4
            m_max0: 32,
        }),
    });
    let result_dim_mismatch = collections_client.create_collection(create_dim_mismatch_request).await;
    assert!(result_dim_mismatch.is_err());
    if let Err(status) = result_dim_mismatch {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Mismatch between CreateCollectionRequest.vector_dimensions"));
    }

    // --- GetCollectionInfo Error Cases ---
    // 8. Test getting info for a non-existent collection
    let get_info_non_existent_req = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: "non_existent_collection".to_string(),
    });
    let result_get_info_non_existent = collections_client.get_collection_info(get_info_non_existent_req).await;
    assert!(result_get_info_non_existent.is_err());
    if let Err(status) = result_get_info_non_existent {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 9. Test GetCollectionInfo with empty collection name
    let get_info_empty_name_req = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: "".to_string(),
    });
    let result_get_info_empty_name = collections_client.get_collection_info(get_info_empty_name_req).await;
    assert!(result_get_info_empty_name.is_err());
    if let Err(status) = result_get_info_empty_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Collection name cannot be empty"));
    }
    
    // --- ListCollections ---
    // (Already tested in successful CRUD, listing empty is also implicitly tested by setup)
    // No specific error cases for ListCollections as it takes no parameters.

    // --- DeleteCollection Error Cases ---
    // 10. Test deleting a non-existent collection
    let delete_non_existent_req = tonic::Request::new(DeleteCollectionRequest {
        collection_name: "non_existent_for_delete".to_string(),
    });
    let result_delete_non_existent = collections_client.delete_collection(delete_non_existent_req).await;
    assert!(result_delete_non_existent.is_err());
    if let Err(status) = result_delete_non_existent {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    // 11. Test deleting with empty collection name
    let delete_empty_name_req = tonic::Request::new(DeleteCollectionRequest {
        collection_name: "".to_string(),
    });
    let result_delete_empty_name = collections_client.delete_collection(delete_empty_name_req).await;
    assert!(result_delete_empty_name.is_err());
    if let Err(status) = result_delete_empty_name {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("Collection name cannot be empty"));
    }
    
    // Clean up the initially created collection if it's still there
    let _ = collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name_str.clone(),
    })).await;

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_collections_service_crud_operations() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) = // Added points_client
        setup_test_server().await?;

    let collection_name_str = "test_collection_crud".to_string();
    let vector_dims = 3;

    let create_request = tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims, // Corrected field name
        distance_metric: DistanceMetric::Cosine as i32, // Explicit cast
        hnsw_config: Some(HnswConfigParams {
            m: 16,
            ef_construction: 100,
            ef_search: 50,
            ml: 0.3,
            seed: None,
            vector_dim: vector_dims, // This is part of HnswConfigParams
            m_max0: 32, // Corrected: M_max0 must be > 0
        }),
        // wal_enabled is not part of CreateCollectionRequest per proto
    });
    let create_response = collections_client
        .create_collection(create_request)
        .await?;
    // CreateCollectionResponse is empty, success is via gRPC status
    assert_eq!(create_response.into_inner(), vortex_server::grpc_api::vortex_api_v1::CreateCollectionResponse {});


    let get_info_request = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_str.clone(), // Corrected field name
    });
    let get_info_response = collections_client
        .get_collection_info(get_info_request)
        .await?
        .into_inner();
    assert_eq!(get_info_response.collection_name, collection_name_str); // Corrected field name
    // GetCollectionInfoResponse has 'config' which is HnswConfigParams, not separate vector_dim
    assert_eq!(get_info_response.config.as_ref().unwrap().vector_dim, vector_dims);
    assert_eq!(
        get_info_response.distance_metric,
        DistanceMetric::Cosine as i32 // Explicit cast
    );
    assert!(get_info_response.config.is_some()); // Corrected field name
    let hnsw_config = get_info_response.config.unwrap(); // Corrected field name
    assert_eq!(hnsw_config.m, 16);
    assert_eq!(hnsw_config.ef_construction, 100);
    assert_eq!(get_info_response.vector_count, 0); // Initially 0 points
    assert_eq!(get_info_response.segment_count, 1); // Initially 1 segment (can be more sophisticated later)


    // Add some points to check vector_count update
    let points_to_add = vec![
        PointStruct { id: "p1".to_string(), vector: Some(Vector { elements: vec![0.1,0.2,0.3] }), payload: None },
        PointStruct { id: "p2".to_string(), vector: Some(Vector { elements: vec![0.4,0.5,0.6] }), payload: None },
    ];
    points_client.upsert_points(tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: points_to_add,
        wait_flush: None,
    })).await?;

    let get_info_request_after_upsert = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_str.clone(),
    });
    let get_info_response_after_upsert = collections_client
        .get_collection_info(get_info_request_after_upsert)
        .await?
        .into_inner();
    assert_eq!(get_info_response_after_upsert.vector_count, 2);
    // Segment count might change depending on implementation, for now assume it's still 1 or more.
    assert!(get_info_response_after_upsert.segment_count >= 1);


    let list_request = tonic::Request::new(ListCollectionsRequest {});
    let list_response = collections_client
        .list_collections(list_request)
        .await?
        .into_inner();
    assert_eq!(list_response.collections.len(), 1);
    assert_eq!(list_response.collections[0].name, collection_name_str);

    // Test CollectionExists (True) by trying to get info
    let get_info_exists_request = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_str.clone(),
    });
    let get_info_exists_result = collections_client
        .get_collection_info(get_info_exists_request)
        .await;
    assert!(get_info_exists_result.is_ok());


    let delete_request = tonic::Request::new(DeleteCollectionRequest { // This is the correct DeleteCollectionRequest
        collection_name: collection_name_str.clone(), // Corrected field name
    });
    let delete_response = collections_client
        .delete_collection(delete_request)
        .await?;
    // DeleteCollectionResponse is empty
    assert_eq!(delete_response.into_inner(), vortex_server::grpc_api::vortex_api_v1::DeleteCollectionResponse {});


    let list_request_after_delete = tonic::Request::new(ListCollectionsRequest {});
    let list_response_after_delete = collections_client
        .list_collections(list_request_after_delete)
        .await?
        .into_inner();
    assert!(list_response_after_delete.collections.is_empty());
    
    // Test CollectionExists (False) by trying to get info
    let get_info_not_exists_request = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_str.clone(),
    });
    let get_info_not_exists_result = collections_client
        .get_collection_info(get_info_not_exists_request)
        .await;
    assert!(get_info_not_exists_result.is_err());
    if let Err(status) = get_info_not_exists_result {
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_points_service_operations() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_str = "test_points_collection".to_string();
    let vector_dims = 3;

    let create_collection_req = tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32, // Explicit cast
        hnsw_config: Some(HnswConfigParams {
            m: 16,
            ef_construction: 100,
            ef_search: 50,
            ml: 0.3,
            seed: None,
            vector_dim: vector_dims,
            m_max0: 32, // Corrected: M_max0 must be > 0
        }),
        // wal_enabled not in proto
    });
    collections_client
        .create_collection(create_collection_req)
        .await?;

    let points_to_upsert = vec![
        PointStruct {
            id: "point1".to_string(),
            vector: Some(Vector {
                elements: vec![1.0, 2.0, 3.0],
            }),
            payload: Some(Payload { // Corrected: PointStruct.payload is Option<Payload>
                fields: [(
                    "color".to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "red".to_string(),
                        )),
                    },
                )]
                .into_iter()
                .collect(),
            }),
        },
        PointStruct {
            id: "point2".to_string(),
            vector: Some(Vector {
                elements: vec![4.0, 5.0, 6.0],
            }),
            payload: Some(Payload { // Corrected: PointStruct.payload is Option<Payload>
                fields: [(
                    "color".to_string(),
                    prost_types::Value {
                        kind: Some(prost_types::value::Kind::StringValue(
                            "blue".to_string(),
                        )),
                    },
                )]
                .into_iter()
                .collect(),
            }),
        },
    ];

    let upsert_request = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: points_to_upsert.clone(),
        wait_flush: Some(true),
    });
    let upsert_response = points_client.upsert_points(upsert_request).await?;
    let upsert_result = upsert_response.into_inner();
    assert_eq!(upsert_result.statuses.len(), 2);
    assert!(upsert_result.statuses.iter().all(|r| r.status_code == (vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32) ));

    let get_points_request = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point1".to_string(), "point2".to_string()],
        with_payload: Some(true),
        with_vector: Some(true),
    });
    let get_points_response = points_client.get_points(get_points_request).await?;
    let retrieved_points = get_points_response.into_inner().points;
    assert_eq!(retrieved_points.len(), 2);

    let search_request = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector {
            elements: vec![1.1, 2.1, 3.1],
        }),
        k_limit: 1,
        filter: None,
        with_payload: Some(true),
        with_vector: Some(false),
        params: Some(SearchParams { ef_search: Some(10) }),
        // offset is not part of SearchPointsRequest
    });
    let search_response = points_client.search_points(search_request).await?;
    let search_results = search_response.into_inner().results;
    assert_eq!(search_results.len(), 1);
    assert_eq!(search_results[0].id, "point1");

    let delete_points_request = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point1".to_string()],
        wait_flush: Some(true), // Corrected field name
    });
    let delete_response = points_client.delete_points(delete_points_request).await?;
    let delete_result = delete_response.into_inner();
    assert_eq!(delete_result.statuses.len(), 1);
    assert!(delete_result.statuses[0].status_code == (vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32));
    
    let get_point1_fail_request = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point1".to_string()],
        with_payload: Some(true),
        with_vector: Some(true),
    });
    let get_point1_fail_response = points_client.get_points(get_point1_fail_request).await?;
    assert!(get_point1_fail_response.into_inner().points.is_empty());

    // Test GetPoints with different with_payload/with_vector combinations for point2
    // Get point2 with vector only
    let get_p2_vec_only_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point2".to_string()],
        with_payload: Some(false),
        with_vector: Some(true),
    });
    let p2_vec_only_resp = points_client.get_points(get_p2_vec_only_req).await?.into_inner();
    assert_eq!(p2_vec_only_resp.points.len(), 1);
    assert!(p2_vec_only_resp.points[0].vector.is_some());
    assert!(p2_vec_only_resp.points[0].payload.is_none());

    // Get point2 with payload only
    let get_p2_payload_only_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point2".to_string()],
        with_payload: Some(true),
        with_vector: Some(false),
    });
    let p2_payload_only_resp = points_client.get_points(get_p2_payload_only_req).await?.into_inner();
    assert_eq!(p2_payload_only_resp.points.len(), 1);
    assert!(p2_payload_only_resp.points[0].vector.is_none());
    assert!(p2_payload_only_resp.points[0].payload.is_some());
    assert_eq!(p2_payload_only_resp.points[0].payload.as_ref().unwrap().fields.get("color").unwrap().kind, 
        Some(prost_types::value::Kind::StringValue("blue".to_string())));


    // Update point2 (vector and payload)
    let updated_point2 = PointStruct {
        id: "point2".to_string(),
        vector: Some(Vector { elements: vec![7.0, 8.0, 9.0] }),
        payload: Some(Payload {
            fields: [(
                "color".to_string(),
                prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("green".to_string())) },
            ), (
                "size".to_string(),
                prost_types::Value { kind: Some(prost_types::value::Kind::NumberValue(10.0)) },
            )]
            .into_iter()
            .collect(),
        }),
    };
    let upsert_update_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: vec![updated_point2.clone()],
        wait_flush: Some(true),
    });
    points_client.upsert_points(upsert_update_req).await?;

    // Get point2 again and verify update
    let get_updated_p2_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["point2".to_string()],
        with_payload: Some(true),
        with_vector: Some(true),
    });
    let updated_p2_resp = points_client.get_points(get_updated_p2_req).await?.into_inner();
    assert_eq!(updated_p2_resp.points.len(), 1);
    let p2_after_update = &updated_p2_resp.points[0];
    assert_eq!(p2_after_update.vector.as_ref().unwrap().elements, vec![7.0, 8.0, 9.0]);
    let payload_fields = &p2_after_update.payload.as_ref().unwrap().fields;
    assert_eq!(payload_fields.get("color").unwrap().kind, Some(prost_types::value::Kind::StringValue("green".to_string())));
    assert_eq!(payload_fields.get("size").unwrap().kind, Some(prost_types::value::Kind::NumberValue(10.0)));


    // Search with a filter
    let search_filter_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![7.1, 8.1, 9.1] }), // Close to updated point2
        k_limit: 1,
        filter: Some(vortex_server::grpc_api::vortex_api_v1::Filter {
            must_match_exact: [(
                "color".to_string(),
                prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("green".to_string())) }
            )].into_iter().collect(),
        }),
        with_payload: Some(true),
        with_vector: Some(false),
        params: None,
    });
    let search_filter_resp = points_client.search_points(search_filter_req).await?.into_inner();
    assert_eq!(search_filter_resp.results.len(), 1);
    assert_eq!(search_filter_resp.results[0].id, "point2");
    assert!(search_filter_resp.results[0].payload.is_some());
    assert_eq!(search_filter_resp.results[0].payload.as_ref().unwrap().fields.get("size").unwrap().kind,
        Some(prost_types::value::Kind::NumberValue(10.0)));

    // Search with a filter that matches no points
    let search_no_match_filter_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![7.1, 8.1, 9.1] }),
        k_limit: 1,
        filter: Some(vortex_server::grpc_api::vortex_api_v1::Filter {
            must_match_exact: [(
                "color".to_string(),
                // This color does not exist for point2
                prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("purple".to_string())) } 
            )].into_iter().collect(),
        }),
        with_payload: Some(true),
        with_vector: Some(false),
        params: None,
    });
    let search_no_match_filter_resp = points_client.search_points(search_no_match_filter_req).await?.into_inner();
    assert!(search_no_match_filter_resp.results.is_empty(), "Search with non-matching filter should yield no results.");


    let delete_collection_req = tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name_str.clone(),
    });
    collections_client
        .delete_collection(delete_collection_req)
        .await?;

    server_handle.abort();
    Ok(())
}
