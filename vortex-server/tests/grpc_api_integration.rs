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
use vortex_server::grpc_services::{CollectionsServerImpl, PointsServerImpl, points_service}; // Added points_service here

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
async fn test_points_service_payload_types() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_str = "test_payload_types_collection".to_string();
    let vector_dims = 2;

    // 1. Create Collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 8, ef_construction: 50, ef_search: 20, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 16,
        }),
    })).await?;
    info!("Collection {} created for payload types test", collection_name_str);

    // 2. Prepare points with diverse payloads
    let mut p1_payload_fields = std::collections::HashMap::new();
    p1_payload_fields.insert("string_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("hello".to_string())) });
    p1_payload_fields.insert("int_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::NumberValue(123.0)) }); // NumberValue is f64
    p1_payload_fields.insert("float_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::NumberValue(45.67)) });
    p1_payload_fields.insert("bool_true_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::BoolValue(true)) });
    p1_payload_fields.insert("bool_false_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::BoolValue(false)) });
    p1_payload_fields.insert("null_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::NullValue(0)) });
    
    let list_values = vec![
        prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("item1".to_string())) },
        prost_types::Value { kind: Some(prost_types::value::Kind::NumberValue(100.0)) },
        prost_types::Value { kind: Some(prost_types::value::Kind::BoolValue(true)) },
    ];
    p1_payload_fields.insert("list_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::ListValue(prost_types::ListValue { values: list_values })) });

    let mut nested_struct_fields = std::collections::BTreeMap::new(); // prost_types::Struct uses BTreeMap
    nested_struct_fields.insert("nested_key".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("nested_value".to_string())) });
    p1_payload_fields.insert("struct_field".to_string(), prost_types::Value { kind: Some(prost_types::value::Kind::StructValue(prost_types::Struct { fields: nested_struct_fields.clone() })) });


    let points_to_upsert = vec![
        PointStruct { 
            id: "p_payload_1".to_string(), 
            vector: Some(Vector { elements: vec![0.1, 0.2] }), 
            payload: Some(Payload { fields: p1_payload_fields.clone() }),
            version: None,
        },
        PointStruct { // Point with no payload
            id: "p_payload_2_no_payload".to_string(),
            vector: Some(Vector { elements: vec![0.3, 0.4] }),
            payload: None,
            version: None,
        },
        PointStruct { // Point with empty payload
            id: "p_payload_3_empty_payload".to_string(),
            vector: Some(Vector { elements: vec![0.5, 0.6] }),
            payload: Some(Payload { fields: std::collections::HashMap::new() }),
            version: None,
        }
    ];

    // 3. Upsert points
    let upsert_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: points_to_upsert.clone(),
        wait_flush: Some(true),
    });
    let upsert_resp = points_client.upsert_points(upsert_req).await?.into_inner();
    assert_eq!(upsert_resp.statuses.len(), 3);
    assert!(upsert_resp.statuses.iter().all(|s| s.status_code == vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32));
    info!("Upserted points with diverse payloads");

    // 4. Get point p_payload_1 and verify its payload
    let get_p1_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_payload_1".to_string()],
        with_payload: Some(true),
        with_vector: Some(false),
    });
    let get_p1_resp = points_client.get_points(get_p1_req).await?.into_inner();
    assert_eq!(get_p1_resp.points.len(), 1);
    let retrieved_p1_payload = get_p1_resp.points[0].payload.as_ref().expect("Payload should exist for p_payload_1");
    
    assert_eq!(retrieved_p1_payload.fields.get("string_field").unwrap().kind, Some(prost_types::value::Kind::StringValue("hello".to_string())));
    assert_eq!(retrieved_p1_payload.fields.get("int_field").unwrap().kind, Some(prost_types::value::Kind::NumberValue(123.0)));
    assert_eq!(retrieved_p1_payload.fields.get("float_field").unwrap().kind, Some(prost_types::value::Kind::NumberValue(45.67)));
    assert_eq!(retrieved_p1_payload.fields.get("bool_true_field").unwrap().kind, Some(prost_types::value::Kind::BoolValue(true)));
    assert_eq!(retrieved_p1_payload.fields.get("bool_false_field").unwrap().kind, Some(prost_types::value::Kind::BoolValue(false)));
    assert_eq!(retrieved_p1_payload.fields.get("null_field").unwrap().kind, Some(prost_types::value::Kind::NullValue(0)));
    
    let retrieved_list = match retrieved_p1_payload.fields.get("list_field").unwrap().kind.as_ref().unwrap() {
        prost_types::value::Kind::ListValue(l) => &l.values,
        _ => panic!("list_field was not a ListValue"),
    };
    assert_eq!(retrieved_list.len(), 3);
    assert_eq!(retrieved_list[0].kind, Some(prost_types::value::Kind::StringValue("item1".to_string())));
    assert_eq!(retrieved_list[1].kind, Some(prost_types::value::Kind::NumberValue(100.0)));
    assert_eq!(retrieved_list[2].kind, Some(prost_types::value::Kind::BoolValue(true)));

    let retrieved_struct = match retrieved_p1_payload.fields.get("struct_field").unwrap().kind.as_ref().unwrap() {
        prost_types::value::Kind::StructValue(s) => &s.fields,
        _ => panic!("struct_field was not a StructValue"),
    };
    assert_eq!(retrieved_struct.get("nested_key").unwrap().kind, Some(prost_types::value::Kind::StringValue("nested_value".to_string())));
    info!("Verified payload for p_payload_1 via GetPoints");

    // 5. Get point p_payload_2_no_payload and verify it has no payload
    let get_p2_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_payload_2_no_payload".to_string()],
        with_payload: Some(true), with_vector: Some(false),
    });
    let get_p2_resp = points_client.get_points(get_p2_req).await?.into_inner();
    assert_eq!(get_p2_resp.points.len(), 1);
    assert!(get_p2_resp.points[0].payload.is_none(), "p_payload_2_no_payload should have no payload");
    info!("Verified p_payload_2_no_payload has no payload");

    // 6. Get point p_payload_3_empty_payload and verify it has an empty payload
    let get_p3_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_payload_3_empty_payload".to_string()],
        with_payload: Some(true), with_vector: Some(false),
    });
    let get_p3_resp = points_client.get_points(get_p3_req).await?.into_inner();
    assert_eq!(get_p3_resp.points.len(), 1);
    let p3_payload = get_p3_resp.points[0].payload.as_ref().expect("p_payload_3_empty_payload should have a payload (with version key)");
    assert_eq!(p3_payload.fields.len(), 1, "Payload for p_payload_3_empty_payload should have 1 field (the version key)");
    assert!(p3_payload.fields.contains_key(points_service::VERSION_KEY), "Payload for p_payload_3_empty_payload should contain the version key");
    info!("Verified p_payload_3_empty_payload has a payload with only the version key");


    // 7. Search for p_payload_1 and verify its payload in search results
    let search_p1_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![0.1, 0.2] }),
        k_limit: 1,
        filter: None,
        with_payload: Some(true),
        with_vector: Some(false),
        params: None,
    });
    let search_p1_resp = points_client.search_points(search_p1_req).await?.into_inner();
    assert_eq!(search_p1_resp.results.len(), 1);
    assert_eq!(search_p1_resp.results[0].id, "p_payload_1");
    let searched_p1_payload = search_p1_resp.results[0].payload.as_ref().expect("Payload should exist for searched p_payload_1");
    assert_eq!(searched_p1_payload.fields.get("string_field").unwrap().kind, Some(prost_types::value::Kind::StringValue("hello".to_string())));
    // ... (can add more assertions for other fields if needed, similar to GetPoints verification)
    info!("Verified payload for p_payload_1 via SearchPoints");

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest{
        collection_name: collection_name_str.clone(),
    })).await?;
    info!("Cleaned up collection {}", collection_name_str);

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_points_service_versioning() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_str = "test_versioning_collection".to_string();
    let vector_dims = 2;

    // 1. Create Collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 8, ef_construction: 50, ef_search: 20, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 16,
        }),
    })).await?;
    info!("Collection {} created for versioning test", collection_name_str);

    // 2. Upsert a new point (p1)
    let point1_initial = PointStruct { 
        id: "p1_version_test".to_string(), 
        vector: Some(Vector { elements: vec![0.1, 0.2] }), 
        payload: Some(Payload {
            fields: [(
                "data".to_string(),
                prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("initial_data".to_string())) }
            )].into_iter().collect()
        }),
        version: None,
    };
    let upsert_p1_initial_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: vec![point1_initial.clone()],
        wait_flush: Some(true),
    });
    let upsert_p1_initial_resp = points_client.upsert_points(upsert_p1_initial_req).await?.into_inner();
    assert_eq!(upsert_p1_initial_resp.statuses.len(), 1);
    assert_eq!(upsert_p1_initial_resp.statuses[0].status_code, vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32);
    info!("Upserted p1 (initial) with wait_flush=true");

    // 3. Search for p1 and verify its version is 1
    let search_p1_req_v1 = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![0.1, 0.2] }),
        k_limit: 1,
        filter: None,
        with_payload: Some(true), 
        with_vector: Some(false),
        params: None,
    });
    let search_p1_resp_v1 = points_client.search_points(search_p1_req_v1).await?.into_inner();
    assert_eq!(search_p1_resp_v1.results.len(), 1);
    assert_eq!(search_p1_resp_v1.results[0].id, "p1_version_test");
    assert_eq!(search_p1_resp_v1.results[0].version, Some(1), "Initial version of p1 should be 1 (via SearchPoints)");
    info!("Verified p1 version is 1 after initial upsert (via SearchPoints): {:?}", search_p1_resp_v1.results[0].payload);

    // Verify with GetPoints
    let get_p1_v1_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p1_version_test".to_string()],
        with_payload: Some(false), with_vector: Some(false),
    });
    let get_p1_v1_resp = points_client.get_points(get_p1_v1_req).await?.into_inner();
    assert_eq!(get_p1_v1_resp.points.len(), 1);
    assert_eq!(get_p1_v1_resp.points[0].id, "p1_version_test");
    assert_eq!(get_p1_v1_resp.points[0].version, Some(1), "Initial version of p1 should be 1 (via GetPoints)");
    info!("Verified p1 version is 1 after initial upsert (via GetPoints)");

    // 4. Upsert p1 again (update)
    let point1_updated = PointStruct { 
        id: "p1_version_test".to_string(), 
        vector: Some(Vector { elements: vec![0.11, 0.22] }), 
        payload: Some(Payload {
            fields: [(
                "data".to_string(),
                prost_types::Value { kind: Some(prost_types::value::Kind::StringValue("updated_data".to_string())) }
            )].into_iter().collect()
        }),
        version: None,
    };
    let upsert_p1_updated_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: vec![point1_updated.clone()],
        wait_flush: Some(true),
    });
    let upsert_p1_updated_resp = points_client.upsert_points(upsert_p1_updated_req).await?.into_inner();
    assert_eq!(upsert_p1_updated_resp.statuses.len(), 1);
    assert_eq!(upsert_p1_updated_resp.statuses[0].status_code, vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32);
    info!("Upserted p1 (updated) with wait_flush=true");

    // 5. Search for p1 and verify its version is 2
    let search_p1_req_v2 = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![0.11, 0.22] }),
        k_limit: 1,
        filter: None,
        with_payload: Some(true),
        with_vector: Some(false),
        params: None,
    });
    let search_p1_resp_v2 = points_client.search_points(search_p1_req_v2).await?.into_inner();
    assert_eq!(search_p1_resp_v2.results.len(), 1);
    assert_eq!(search_p1_resp_v2.results[0].id, "p1_version_test");
    assert_eq!(search_p1_resp_v2.results[0].version, Some(2), "Updated version of p1 should be 2 (via SearchPoints)");
    info!("Verified p1 version is 2 after update (via SearchPoints): {:?}", search_p1_resp_v2.results[0].payload);

    // Verify with GetPoints
    let get_p1_v2_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p1_version_test".to_string()],
        with_payload: Some(false), with_vector: Some(false),
    });
    let get_p1_v2_resp = points_client.get_points(get_p1_v2_req).await?.into_inner();
    assert_eq!(get_p1_v2_resp.points.len(), 1);
    assert_eq!(get_p1_v2_resp.points[0].id, "p1_version_test");
    assert_eq!(get_p1_v2_resp.points[0].version, Some(2), "Updated version of p1 should be 2 (via GetPoints)");
    info!("Verified p1 version is 2 after update (via GetPoints)");

    // 6. Upsert another new point (p2) and an update to p1 in the same batch
    let point2_new = PointStruct {
        id: "p2_version_test".to_string(),
        vector: Some(Vector { elements: vec![0.5, 0.5] }),
        payload: None,
        version: None,
    };
    let point1_further_updated = PointStruct {
        id: "p1_version_test".to_string(), 
        vector: Some(Vector { elements: vec![0.111, 0.222] }),
        payload: None,
        version: None,
    };
    let batch_upsert_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: vec![point2_new.clone(), point1_further_updated.clone()],
        wait_flush: Some(true),
    });
    points_client.upsert_points(batch_upsert_req).await?;

    // 7. Search for p1 and p2 and verify their versions
    let search_batch_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name_str.clone(),
        query_vector: Some(Vector { elements: vec![0.0, 0.0] }), 
        k_limit: 2, 
        filter: None,
        with_payload: Some(false),
        with_vector: Some(false),
        params: None,
    });
    let search_batch_resp = points_client.search_points(search_batch_req).await?.into_inner();
    assert_eq!(search_batch_resp.results.len(), 2);

    for res_point in search_batch_resp.results {
        if res_point.id == "p1_version_test" {
            assert_eq!(res_point.version, Some(3), "Version of p1 after batch update should be 3");
        } else if res_point.id == "p2_version_test" {
            assert_eq!(res_point.version, None, "Version of new p2 (no payload) should be None (via SearchPoints)");
        } else {
            panic!("Unexpected point ID in search results: {}", res_point.id);
        }
    }
    info!("Verified versions after batch upsert (via SearchPoints) (p1: Some(3), p2: None)");

    // Verify with GetPoints after batch
    let get_p1_v3_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p1_version_test".to_string()],
        with_payload: Some(false), with_vector: Some(false),
    });
    let get_p1_v3_resp = points_client.get_points(get_p1_v3_req).await?.into_inner();
    assert_eq!(get_p1_v3_resp.points.len(), 1);
    assert_eq!(get_p1_v3_resp.points[0].version, Some(3), "Version of p1 after batch update should be 3 (via GetPoints)");

    let get_p2_v_none_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p2_version_test".to_string()],
        with_payload: Some(false), with_vector: Some(false),
    });
    let get_p2_v_none_resp = points_client.get_points(get_p2_v_none_req).await?.into_inner();
    assert_eq!(get_p2_v_none_resp.points.len(), 1);
    assert_eq!(get_p2_v_none_resp.points[0].version, None, "Version of p2 (no payload) should be None (via GetPoints)");
    info!("Verified versions after batch upsert (via GetPoints) (p1: Some(3), p2: None)");

    // 8. Update p1 to have no payload (None)
    let point1_payload_none = PointStruct {
        id: "p1_version_test".to_string(),
        vector: Some(Vector { elements: vec![0.111, 0.222] }), // Vector can be same or different
        payload: None, // Explicitly None
        version: None,
    };
    let upsert_p1_payload_none_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: vec![point1_payload_none.clone()],
        wait_flush: Some(true),
    });
    points_client.upsert_points(upsert_p1_payload_none_req).await?;
    info!("Upserted p1 with payload: None");

    // 9. Get p1 and verify its version is 4, and payload contains only the version key
    let get_p1_v4_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p1_version_test".to_string()],
        with_payload: Some(true), // Need payload to check its content
        with_vector: Some(false),
    });
    let get_p1_v4_resp = points_client.get_points(get_p1_v4_req).await?.into_inner();
    assert_eq!(get_p1_v4_resp.points.len(), 1, "p1 should exist after updating to None payload");
    let p1_after_none_payload = &get_p1_v4_resp.points[0];
    assert_eq!(p1_after_none_payload.version, Some(4), "Version of p1 after setting payload to None should be 4");
    
    let p1_payload_fields = p1_after_none_payload.payload.as_ref().expect("Payload should exist for p1 (version only)").fields.clone();
    assert_eq!(p1_payload_fields.len(), 1, "Payload for p1 should only contain the version key");
    assert!(p1_payload_fields.contains_key(points_service::VERSION_KEY), "Payload must contain version key");
    match p1_payload_fields.get(points_service::VERSION_KEY).unwrap().kind.as_ref().unwrap() {
        prost_types::value::Kind::NumberValue(n) => assert_eq!(*n, 4.0),
        _ => panic!("Version key should be a number value"),
    }
    info!("Verified p1 version is 4 and payload contains only version after setting payload to None");


    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest{
        collection_name: collection_name_str.clone(),
    })).await?;
    info!("Cleaned up collection {}", collection_name_str);

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_points_service_wait_flush_operations() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_str = "test_wait_flush_collection".to_string();
    let vector_dims = 2;

    // 1. Create Collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_str.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(HnswConfigParams {
            m: 8, ef_construction: 50, ef_search: 20, ml: 0.3, seed: None, vector_dim: vector_dims, m_max0: 16,
        }),
    })).await?;
    info!("Collection {} created for wait_flush test", collection_name_str);

    // 2. Upsert points with wait_flush = true
    let points_flush_true = vec![
        PointStruct { id: "p_flush_1".to_string(), vector: Some(Vector { elements: vec![0.1, 0.2] }), payload: None, version: None },
        PointStruct { id: "p_flush_2".to_string(), vector: Some(Vector { elements: vec![0.3, 0.4] }), payload: None, version: None },
    ];
    let upsert_flush_true_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: points_flush_true.clone(),
        wait_flush: Some(true),
    });
    let upsert_flush_true_resp = points_client.upsert_points(upsert_flush_true_req).await?.into_inner();
    assert_eq!(upsert_flush_true_resp.statuses.len(), 2);
    assert!(upsert_flush_true_resp.statuses.iter().all(|s| s.status_code == vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32));
    info!("Upserted points with wait_flush=true");

    // Verify points exist
    let get_req_flush_true = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_flush_1".to_string(), "p_flush_2".to_string()],
        with_vector: Some(false), with_payload: Some(false),
    });
    let get_resp_flush_true = points_client.get_points(get_req_flush_true).await?.into_inner();
    assert_eq!(get_resp_flush_true.points.len(), 2, "Points upserted with wait_flush=true not found");

    // 3. Upsert points with wait_flush = false
    let points_flush_false = vec![
        PointStruct { id: "p_noflush_1".to_string(), vector: Some(Vector { elements: vec![0.5, 0.6] }), payload: None, version: None },
    ];
    let upsert_flush_false_req = tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name_str.clone(),
        points: points_flush_false.clone(),
        wait_flush: Some(false),
    });
    let upsert_flush_false_resp = points_client.upsert_points(upsert_flush_false_req).await?.into_inner();
    assert_eq!(upsert_flush_false_resp.statuses.len(), 1);
    assert!(upsert_flush_false_resp.statuses[0].status_code == vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32);
    info!("Upserted points with wait_flush=false");
    
    // Verify points exist
    let get_req_flush_false = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_noflush_1".to_string()],
        with_vector: Some(false), with_payload: Some(false),
    });
    let get_resp_flush_false = points_client.get_points(get_req_flush_false).await?.into_inner();
    assert_eq!(get_resp_flush_false.points.len(), 1, "Point upserted with wait_flush=false not found");


    // 4. Delete points with wait_flush = true
    let delete_flush_true_req = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_flush_1".to_string()],
        wait_flush: Some(true),
    });
    let delete_flush_true_resp = points_client.delete_points(delete_flush_true_req).await?.into_inner();
    assert_eq!(delete_flush_true_resp.statuses.len(), 1);
    assert!(delete_flush_true_resp.statuses[0].status_code == vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32);
    info!("Deleted point p_flush_1 with wait_flush=true");

    // Verify p_flush_1 is deleted
    let get_deleted_p_flush_1_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_flush_1".to_string()],
        with_vector: Some(false), with_payload: Some(false),
    });
    let get_deleted_p_flush_1_resp = points_client.get_points(get_deleted_p_flush_1_req).await?.into_inner();
    assert!(get_deleted_p_flush_1_resp.points.is_empty(), "Point p_flush_1 was not deleted with wait_flush=true");

    // 5. Delete points with wait_flush = false
    let delete_flush_false_req = tonic::Request::new(GrpcDeletePointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_flush_2".to_string()],
        wait_flush: Some(false),
    });
    let delete_flush_false_resp = points_client.delete_points(delete_flush_false_req).await?.into_inner();
    assert_eq!(delete_flush_false_resp.statuses.len(), 1);
    assert!(delete_flush_false_resp.statuses[0].status_code == vortex_server::grpc_api::vortex_api_v1::StatusCode::Ok as i32);
    info!("Deleted point p_flush_2 with wait_flush=false");

    // Verify p_flush_2 is deleted
    let get_deleted_p_flush_2_req = tonic::Request::new(GetPointsRequest {
        collection_name: collection_name_str.clone(),
        ids: vec!["p_flush_2".to_string()],
        with_vector: Some(false), with_payload: Some(false),
    });
    let get_deleted_p_flush_2_resp = points_client.get_points(get_deleted_p_flush_2_req).await?.into_inner();
    assert!(get_deleted_p_flush_2_resp.points.is_empty(), "Point p_flush_2 was not deleted with wait_flush=false");

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest{
        collection_name: collection_name_str.clone(),
    })).await?;
    info!("Cleaned up collection {}", collection_name_str);

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
            version: None,
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
            version: None,
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
            version: None,
        },
        PointStruct { // Invalid: dimension mismatch
            id: "invalid_dim_p2".to_string(),
            vector: Some(Vector { elements: vec![1.0, 2.0] }), // Expected 3 dims
            payload: None,
            version: None,
        },
        PointStruct { // Invalid: empty ID
            id: "".to_string(),
            vector: Some(Vector { elements: vec![4.0, 5.0, 6.0] }),
            payload: None,
            version: None,
        },
        PointStruct { // Valid
            id: "valid_p3".to_string(),
            vector: Some(Vector { elements: vec![7.0, 8.0, 9.0] }),
            payload: None,
            version: None,
        },
        PointStruct { // Invalid: missing vector
            id: "invalid_missing_vec_p4".to_string(),
            vector: None,
            payload: None,
            version: None,
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
async fn test_search_points_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, mut points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name = "search_edges_collection".to_string();
    let vector_dims = 2;

    // Create collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: None,
    })).await?;

    // 1. Search on an empty collection
    let search_empty_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name.clone(),
        query_vector: Some(Vector { elements: vec![1.0, 1.0] }),
        k_limit: 5,
        filter: None, with_payload: None, with_vector: None, params: None,
    });
    let search_empty_resp = points_client.search_points(search_empty_req).await?.into_inner();
    assert!(search_empty_resp.results.is_empty(), "Search on empty collection should return no results");
    info!("Verified search on empty collection");

    // Add one point
    let point1 = PointStruct {
        id: "p_edge_1".to_string(),
        vector: Some(Vector { elements: vec![0.1, 0.2] }),
        payload: None, version: None,
    };
    points_client.upsert_points(tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name.clone(),
        points: vec![point1.clone()],
        wait_flush: Some(true),
    })).await?;

    // 2. Search with k_limit greater than available points
    let search_k_gt_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name.clone(),
        query_vector: Some(Vector { elements: vec![0.1, 0.2] }),
        k_limit: 10, // k > number of points (1)
        filter: None, with_payload: None, with_vector: None, params: None,
    });
    let search_k_gt_resp = points_client.search_points(search_k_gt_req).await?.into_inner();
    assert_eq!(search_k_gt_resp.results.len(), 1, "Search with k > num_points should return all points");
    assert_eq!(search_k_gt_resp.results[0].id, "p_edge_1");
    info!("Verified search with k_limit > available points");

    // Add another point
     let point2 = PointStruct {
        id: "p_edge_2".to_string(),
        vector: Some(Vector { elements: vec![0.9, 0.8] }),
        payload: None, version: None,
    };
    points_client.upsert_points(tonic::Request::new(UpsertPointsRequest {
        collection_name: collection_name.clone(),
        points: vec![point2.clone()],
        wait_flush: Some(true),
    })).await?;
    
    // 3. Search with specific ef_search
    let ef_search_val = 5_u32; // A small value for ef_search
    let search_ef_req = tonic::Request::new(SearchPointsRequest {
        collection_name: collection_name.clone(),
        query_vector: Some(Vector { elements: vec![0.1, 0.1] }),
        k_limit: 1,
        filter: None, with_payload: None, with_vector: None, 
        params: Some(SearchParams { ef_search: Some(ef_search_val) }),
    });
    let search_ef_resp = points_client.search_points(search_ef_req).await?.into_inner();
    // We can't directly verify ef_search was used, but the call should succeed
    assert!(!search_ef_resp.results.is_empty(), "Search with custom ef_search should return results"); 
    info!("Verified search with custom ef_search parameter");

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name.clone(),
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
async fn test_collections_service_create_with_default_hnsw_config() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, _points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name = "test_default_hnsw_coll".to_string();
    let vector_dims = 5u32;

    // Create collection with hnsw_config: None
    let create_req = tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name.clone(),
        vector_dimensions: vector_dims,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: None, // Explicitly None to test default config
    });
    collections_client.create_collection(create_req).await?;
    info!("Collection {} created with default HNSW config", collection_name);

    // Get info and verify default HNSW parameters are applied
    let get_info_req = tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name.clone(),
    });
    let info_resp = collections_client.get_collection_info(get_info_req).await?.into_inner();
    
    assert_eq!(info_resp.collection_name, collection_name);
    assert!(info_resp.config.is_some(), "HNSW config should be present even if defaulted");
    
    let mut default_core_config = vortex_core::HnswConfig::default();
    default_core_config.vector_dim = vector_dims; // Set the dimension as the service does
    let returned_config = info_resp.config.unwrap();

    assert_eq!(returned_config.m, default_core_config.m as u32);
    assert_eq!(returned_config.m_max0, default_core_config.m_max0 as u32);
    assert_eq!(returned_config.ef_construction, default_core_config.ef_construction as u32);
    assert_eq!(returned_config.ef_search, default_core_config.ef_search as u32); // ef_search should also be default
    assert_eq!(returned_config.ml, default_core_config.ml);
    assert_eq!(returned_config.seed, default_core_config.seed);
    assert_eq!(returned_config.vector_dim, default_core_config.vector_dim);

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name.clone(),
    })).await?;
    info!("Cleaned up collection {}", collection_name);

    server_handle.abort();
    Ok(())
}

#[tokio::test]
async fn test_collections_service_varied_hnsw_params() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt::try_init();
    let (mut collections_client, _points_client, _addr, server_handle) =
        setup_test_server().await?;

    let collection_name_1 = "test_varied_hnsw_1".to_string();
    let vector_dims_1 = 4;
    let hnsw_config_1 = HnswConfigParams {
        m: 32, ef_construction: 200, ef_search: 100, ml: 0.5, seed: Some(12345), vector_dim: vector_dims_1, m_max0: 64,
    };

    // Create first collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_1.clone(),
        vector_dimensions: vector_dims_1,
        distance_metric: DistanceMetric::EuclideanL2 as i32,
        hnsw_config: Some(hnsw_config_1.clone()),
    })).await?;

    // Get info for first collection and verify params
    let info1_resp = collections_client.get_collection_info(tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_1.clone(),
    })).await?.into_inner();
    assert_eq!(info1_resp.collection_name, collection_name_1);
    assert_eq!(info1_resp.config.as_ref().unwrap().m, hnsw_config_1.m);
    assert_eq!(info1_resp.config.as_ref().unwrap().ef_construction, hnsw_config_1.ef_construction);
    assert_eq!(info1_resp.config.as_ref().unwrap().ef_search, hnsw_config_1.ef_search);
    assert_eq!(info1_resp.config.as_ref().unwrap().ml, hnsw_config_1.ml);
    assert_eq!(info1_resp.config.as_ref().unwrap().seed, hnsw_config_1.seed);
    assert_eq!(info1_resp.config.as_ref().unwrap().vector_dim, hnsw_config_1.vector_dim);
    assert_eq!(info1_resp.config.as_ref().unwrap().m_max0, hnsw_config_1.m_max0);
    assert_eq!(info1_resp.distance_metric, DistanceMetric::EuclideanL2 as i32);

    let collection_name_2 = "test_varied_hnsw_2".to_string();
    let vector_dims_2 = 8;
    let hnsw_config_2 = HnswConfigParams {
        m: 12, ef_construction: 80, ef_search: 40, ml: 0.25, seed: None, vector_dim: vector_dims_2, m_max0: 24,
    };

    // Create second collection
    collections_client.create_collection(tonic::Request::new(CreateCollectionRequest {
        collection_name: collection_name_2.clone(),
        vector_dimensions: vector_dims_2,
        distance_metric: DistanceMetric::Cosine as i32,
        hnsw_config: Some(hnsw_config_2.clone()),
    })).await?;
    
    // Get info for second collection and verify params
    let info2_resp = collections_client.get_collection_info(tonic::Request::new(GetCollectionInfoRequest {
        collection_name: collection_name_2.clone(),
    })).await?.into_inner();
    assert_eq!(info2_resp.collection_name, collection_name_2);
    assert_eq!(info2_resp.config.as_ref().unwrap().m, hnsw_config_2.m);
    assert_eq!(info2_resp.config.as_ref().unwrap().ef_construction, hnsw_config_2.ef_construction);
    assert_eq!(info2_resp.config.as_ref().unwrap().vector_dim, hnsw_config_2.vector_dim);
    assert_eq!(info2_resp.distance_metric, DistanceMetric::Cosine as i32);

    // List collections and verify both exist
    let list_resp = collections_client.list_collections(tonic::Request::new(ListCollectionsRequest {})).await?.into_inner();
    assert_eq!(list_resp.collections.len(), 2);
    assert!(list_resp.collections.iter().any(|c| c.name == collection_name_1));
    assert!(list_resp.collections.iter().any(|c| c.name == collection_name_2));

    // Clean up
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name_1.clone(),
    })).await?;
    collections_client.delete_collection(tonic::Request::new(DeleteCollectionRequest {
        collection_name: collection_name_2.clone(),
    })).await?;

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
        PointStruct { id: "p1".to_string(), vector: Some(Vector { elements: vec![0.1,0.2,0.3] }), payload: None, version: None },
        PointStruct { id: "p2".to_string(), vector: Some(Vector { elements: vec![0.4,0.5,0.6] }), payload: None, version: None },
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
            version: None,
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
            version: None,
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
    for point in &retrieved_points {
        assert_eq!(point.version, Some(1), "Initial version should be 1 for point {}", point.id);
    }

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
    assert_eq!(search_results[0].version, Some(1), "Version for point1 in search should be 1");

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
        version: None,
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
    assert_eq!(p2_after_update.version, Some(2), "Version of point2 after update should be 2");


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
    assert_eq!(search_filter_resp.results[0].version, Some(2), "Version of point2 in filtered search should be 2");

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
