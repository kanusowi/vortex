"""
Unit tests for the VortexClient.
"""
import pytest
import grpc # type: ignore
from unittest.mock import MagicMock, patch

from vortex_sdk import VortexClient, models
from vortex_sdk.exceptions import VortexApiError, VortexConnectionError, VortexException, VortexClientConfigurationError
from vortex_sdk._grpc.vortex.api.v1 import collections_service_pb2
from vortex_sdk._grpc.vortex.api.v1 import points_service_pb2
from vortex_sdk._grpc.vortex.api.v1 import common_pb2

@pytest.fixture
def mock_grpc_channel():
    return MagicMock(spec=grpc.Channel)

@pytest.fixture
def mock_collections_stub(mock_grpc_channel):
    stub = MagicMock()
    # Attach to a mock channel if VortexClient's _connect tries to use it
    mock_grpc_channel.collections_stub = stub 
    return stub

@pytest.fixture
def mock_points_stub(mock_grpc_channel):
    stub = MagicMock()
    mock_grpc_channel.points_stub = stub
    return stub

@pytest.fixture
def client(mocker, request, mock_grpc_channel, mock_collections_stub, mock_points_stub): # Added 'request'
    """Fixture to create a VortexClient with mocked gRPC stubs."""
    mocker.patch('grpc.insecure_channel', return_value=mock_grpc_channel)
    
    # Mock the stubs that would be created in _connect
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)
    
    # For most tests, we mock _connect to avoid actual channel creation.
    # For connection-specific tests, we'll mock grpc.insecure_channel/secure_channel directly.
    if 'skip_connect_mock' not in request.keywords:
        mocker.patch('vortex_sdk.client.VortexClient._connect', MagicMock())

    vc = VortexClient(host="mockhost", port=12345)
    
    if 'skip_connect_mock' not in request.keywords:
        # Manually assign stubs if _connect is fully mocked
        vc._channel = mock_grpc_channel
        vc._collections_stub = mock_collections_stub
        vc._points_stub = mock_points_stub
    return vc

# --- Connection Tests ---

@pytest.mark.skip_connect_mock # Custom marker to skip the _connect mock in the client fixture
def test_client_connect_insecure(mocker, mock_collections_stub, mock_points_stub):
    """Test insecure channel creation."""
    mock_insecure_channel = mocker.patch('grpc.insecure_channel', return_value=MagicMock(spec=grpc.Channel))
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)

    options = [("grpc.lb_policy_name", "pick_first")]
    client = VortexClient(host="test_host", port=123, secure=False, grpc_options=options)
    
    mock_insecure_channel.assert_called_once_with("test_host:123", options=options)
    assert client._channel == mock_insecure_channel.return_value
    assert client._collections_stub is not None
    assert client._points_stub is not None

@pytest.mark.skip_connect_mock
def test_client_connect_secure_server_auth(mocker, mock_collections_stub, mock_points_stub):
    """Test secure channel creation with server authentication (root_certs)."""
    mock_secure_channel = mocker.patch('grpc.secure_channel', return_value=MagicMock(spec=grpc.Channel))
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials))
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)

    root_certs_data = b"test_root_certs"
    options = [("grpc.ssl_target_name_override", "server.name")]
    
    client = VortexClient(
        host="secure_host", port=443, secure=True, 
        root_certs=root_certs_data, grpc_options=options
    )
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=root_certs_data,
        private_key=None,
        certificate_chain=None
    )
    mock_secure_channel.assert_called_once_with(
        "secure_host:443", mock_ssl_creds.return_value, options=options
    )
    assert client._channel == mock_secure_channel.return_value

@pytest.mark.skip_connect_mock
def test_client_connect_secure_mtls(mocker, mock_collections_stub, mock_points_stub):
    """Test secure channel creation with mutual TLS."""
    mock_secure_channel = mocker.patch('grpc.secure_channel', return_value=MagicMock(spec=grpc.Channel))
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials))
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)

    root_certs_data = b"test_root_certs_mtls"
    private_key_data = b"test_private_key_mtls"
    cert_chain_data = b"test_cert_chain_mtls"
    
    client = VortexClient(
        host="mtls_host", port=443, secure=True, 
        root_certs=root_certs_data, private_key=private_key_data, certificate_chain=cert_chain_data
    )
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=root_certs_data,
        private_key=private_key_data,
        certificate_chain=cert_chain_data
    )
    mock_secure_channel.assert_called_once_with(
        "mtls_host:443", mock_ssl_creds.return_value, options=None # No options passed here
    )

@pytest.mark.skip_connect_mock
def test_client_connect_secure_system_ca(mocker, mock_collections_stub, mock_points_stub):
    """Test secure channel creation using system CAs (secure=True, no certs provided)."""
    mock_secure_channel = mocker.patch('grpc.secure_channel', return_value=MagicMock(spec=grpc.Channel))
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials))
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)
    
    client = VortexClient(host="systemca_host", port=443, secure=True)
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=None, private_key=None, certificate_chain=None
    )
    mock_secure_channel.assert_called_once_with(
        "systemca_host:443", mock_ssl_creds.return_value, options=None
    )

# --- CollectionsService Method Tests ---

def test_create_collection_success(client, mock_collections_stub):
    """Test successful collection creation."""
    mock_collections_stub.CreateCollection.return_value = collections_service_pb2.CreateCollectionResponse()
    
    client.create_collection(
        collection_name="test_coll",
        vector_dimensions=128,
        distance_metric=models.DistanceMetric.COSINE,
        hnsw_config=models.HnswConfigParams(m=16, ef_construction=100, ef_search=50, ml=0.5, vector_dim=128, m_max0=32)
    )
    
    mock_collections_stub.CreateCollection.assert_called_once()
    call_args = mock_collections_stub.CreateCollection.call_args[0][0]
    assert call_args.collection_name == "test_coll"
    assert call_args.vector_dimensions == 128
    assert call_args.distance_metric == common_pb2.DistanceMetric.COSINE
    assert call_args.hnsw_config.m == 16

def test_create_collection_api_error(client, mock_collections_stub):
    """Test collection creation failure with RpcError."""
    mock_error = grpc.RpcError("Mock gRPC error")
    mock_error.code = lambda: grpc.StatusCode.INTERNAL # Add code() method
    mock_collections_stub.CreateCollection.side_effect = mock_error
    
    with pytest.raises(VortexApiError, match="Failed to create collection 'test_coll_fail'"):
        client.create_collection(
            collection_name="test_coll_fail",
            vector_dimensions=128,
            distance_metric=models.DistanceMetric.EUCLIDEAN_L2
        )

def test_get_collection_info_success(client, mock_collections_stub):
    """Test successfully getting collection info."""
    mock_response = collections_service_pb2.GetCollectionInfoResponse(
        collection_name="info_coll",
        status=collections_service_pb2.CollectionStatus.GREEN,
        vector_count=100,
        config=common_pb2.HnswConfigParams(m=8, ef_construction=50, ef_search=20, ml=0.3, vector_dim=64, m_max0=16),
        distance_metric=common_pb2.DistanceMetric.EUCLIDEAN_L2
    )
    mock_collections_stub.GetCollectionInfo.return_value = mock_response
    
    info = client.get_collection_info("info_coll")
    
    assert info.collection_name == "info_coll"
    assert info.status == models.CollectionStatus.GREEN
    assert info.vector_count == 100
    assert info.config.m == 8
    assert info.distance_metric == models.DistanceMetric.EUCLIDEAN_L2
    mock_collections_stub.GetCollectionInfo.assert_called_once_with(
        collections_service_pb2.GetCollectionInfoRequest(collection_name="info_coll"),
        timeout=None 
    )

def test_list_collections_success(client, mock_collections_stub):
    """Test successfully listing collections."""
    mock_response = collections_service_pb2.ListCollectionsResponse(
        collections=[
            collections_service_pb2.CollectionDescription(name="coll1", vector_count=10, status=collections_service_pb2.CollectionStatus.GREEN, dimensions=128, distance_metric=common_pb2.DistanceMetric.COSINE),
            collections_service_pb2.CollectionDescription(name="coll2", vector_count=20, status=collections_service_pb2.CollectionStatus.YELLOW, dimensions=64, distance_metric=common_pb2.DistanceMetric.EUCLIDEAN_L2),
        ]
    )
    mock_collections_stub.ListCollections.return_value = mock_response
    
    descriptions = client.list_collections()
    
    assert len(descriptions) == 2
    assert descriptions[0].name == "coll1"
    assert descriptions[0].status == models.CollectionStatus.GREEN
    assert descriptions[1].name == "coll2"
    assert descriptions[1].distance_metric == models.DistanceMetric.EUCLIDEAN_L2
    mock_collections_stub.ListCollections.assert_called_once()

def test_delete_collection_success(client, mock_collections_stub):
    """Test successful collection deletion."""
    mock_collections_stub.DeleteCollection.return_value = collections_service_pb2.DeleteCollectionResponse()
    
    client.delete_collection("delete_me")
    
    mock_collections_stub.DeleteCollection.assert_called_once_with(
        collections_service_pb2.DeleteCollectionRequest(collection_name="delete_me"),
        timeout=None
    )

# --- PointsService Method Tests (Basic Placeholders) ---

def test_upsert_points_success(client, mock_points_stub):
    """Test successful point upsertion."""
    points_to_upsert = [
        models.PointStruct(id="p1", vector=models.Vector(elements=[0.1, 0.2]))
    ]
    mock_response = points_service_pb2.UpsertPointsResponse(
        statuses=[common_pb2.PointOperationStatus(point_id="p1", status_code=common_pb2.StatusCode.OK)]
    )
    mock_points_stub.UpsertPoints.return_value = mock_response

    statuses = client.upsert_points(collection_name="upsert_coll", points=points_to_upsert)

    assert len(statuses) == 1
    assert statuses[0].point_id == "p1"
    assert statuses[0].status_code == models.StatusCode.OK
    mock_points_stub.UpsertPoints.assert_called_once()
    call_args = mock_points_stub.UpsertPoints.call_args[0][0]
    assert call_args.collection_name == "upsert_coll"
    assert len(call_args.points) == 1
    assert call_args.points[0].id == "p1"

def test_upsert_points_overall_error(client, mock_points_stub):
    """Test point upsertion with an overall error."""
    points_to_upsert = [models.PointStruct(id="p1", vector=models.Vector(elements=[0.1, 0.2]))]
    mock_response = points_service_pb2.UpsertPointsResponse(overall_error="WAL is full")
    mock_points_stub.UpsertPoints.return_value = mock_response

    with pytest.raises(VortexApiError, match="Overall error during upsert: WAL is full"):
        client.upsert_points(collection_name="upsert_coll_fail", points=points_to_upsert)


def test_get_points_success(client, mock_points_stub):
    """Test successfully getting points."""
    mock_response = points_service_pb2.GetPointsResponse(
        points=[
            common_pb2.PointStruct(id="gp1", vector=common_pb2.Vector(elements=[0.3, 0.4]))
        ]
    )
    mock_points_stub.GetPoints.return_value = mock_response

    points = client.get_points(collection_name="get_coll", ids=["gp1"], with_vector=True)
    
    assert len(points) == 1
    assert points[0].id == "gp1"
    assert points[0].vector.elements == pytest.approx([0.3, 0.4])
    mock_points_stub.GetPoints.assert_called_once()
    call_args = mock_points_stub.GetPoints.call_args[0][0]
    assert call_args.collection_name == "get_coll"
    assert list(call_args.ids) == ["gp1"]
    assert call_args.with_vector is True
    assert call_args.with_payload is True # Default

def test_delete_points_success(client, mock_points_stub):
    """Test successful point deletion."""
    mock_response = points_service_pb2.DeletePointsResponse(
        statuses=[common_pb2.PointOperationStatus(point_id="dp1", status_code=common_pb2.StatusCode.OK)]
    )
    mock_points_stub.DeletePoints.return_value = mock_response

    statuses = client.delete_points(collection_name="del_coll", ids=["dp1"])

    assert len(statuses) == 1
    assert statuses[0].point_id == "dp1"
    mock_points_stub.DeletePoints.assert_called_once()

def test_search_points_success(client, mock_points_stub):
    """Test successful point search."""
    query_vec = models.Vector(elements=[0.5, 0.6])
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[
            common_pb2.ScoredPoint(id="sp1", score=0.9, vector=common_pb2.Vector(elements=[0.51, 0.61]))
        ]
    )
    mock_points_stub.SearchPoints.return_value = mock_response

    results = client.search_points(
        collection_name="search_coll", 
        query_vector=query_vec, 
        k_limit=5,
        with_vector=True
    )

    assert len(results) == 1
    assert results[0].id == "sp1"
    assert results[0].score == pytest.approx(0.9)
    assert results[0].vector.elements == pytest.approx([0.51, 0.61])
    mock_points_stub.SearchPoints.assert_called_once()
    call_args = mock_points_stub.SearchPoints.call_args[0][0]
    assert call_args.collection_name == "search_coll"
    assert list(call_args.query_vector.elements) == pytest.approx([0.5, 0.6])
    assert call_args.k_limit == 5
    assert call_args.with_vector is True
    assert not call_args.HasField("params") # Ensure params is not set by default

def test_search_points_with_search_params(client, mock_points_stub):
    """Test point search with SearchParams."""
    query_vec = models.Vector(elements=[0.7, 0.8])
    search_p = models.SearchParams(ef_search=150)
    
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[
            common_pb2.ScoredPoint(id="sp_ef", score=0.95)
        ]
    )
    mock_points_stub.SearchPoints.return_value = mock_response

    results = client.search_points(
        collection_name="search_params_coll",
        query_vector=query_vec,
        k_limit=3,
        search_params=search_p
    )
    assert len(results) == 1
    mock_points_stub.SearchPoints.assert_called_once()
    call_args = mock_points_stub.SearchPoints.call_args[0][0]
    assert call_args.collection_name == "search_params_coll"
    assert call_args.HasField("params")
    assert call_args.params.ef_search == 150

def test_search_points_with_empty_search_params(client, mock_points_stub):
    """Test point search with SearchParams that results in None after conversion."""
    query_vec = models.Vector(elements=[0.7, 0.8])
    search_p_empty = models.SearchParams() # ef_search is None
    
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[common_pb2.ScoredPoint(id="sp_empty_ef", score=0.93)]
    )
    mock_points_stub.SearchPoints.return_value = mock_response

    client.search_points(
        collection_name="search_empty_params_coll",
        query_vector=query_vec,
        k_limit=2,
        search_params=search_p_empty
    )
    mock_points_stub.SearchPoints.assert_called_once()
    call_args = mock_points_stub.SearchPoints.call_args[0][0]
    assert not call_args.HasField("params") # params should not be set if conversion returns None


def test_client_no_connection(mocker):
    """Test that methods raise VortexConnectionError if stubs are None (simulating no connection)."""
    mocker.patch('grpc.insecure_channel', side_effect=grpc.RpcError("Connection failed during init"))
    
    # This will fail in _connect and stubs will remain None
    with pytest.raises(VortexConnectionError):
        VortexClient(host="badhost") # This call itself should raise the error

def test_methods_raise_if_stubs_are_none(client):
    """Test that methods raise VortexConnectionError if stubs are None after successful init."""
    # Simulate stubs not being available post-init for some reason
    client._collections_stub = None
    client._points_stub = None

    with pytest.raises(VortexConnectionError, match="Client not connected"):
        client.list_collections()
    
    with pytest.raises(VortexConnectionError, match="Client not connected"):
        client.search_points("any", models.Vector(elements=[]), 1)

# --- Retry Logic Tests (_execute_with_retry) ---

@pytest.fixture
def mock_grpc_call():
    return MagicMock()

def test_retry_disabled_successful_call(client, mock_grpc_call):
    """Test successful call when retries are disabled."""
    client.retries_enabled = False
    mock_grpc_call.return_value = "success"
    
    result = client._execute_with_retry(mock_grpc_call, "test_op", "arg1", kwarg1="kwval1")
    
    assert result == "success"
    mock_grpc_call.assert_called_once_with("arg1", kwarg1="kwval1")

def test_retry_disabled_grpc_error(client, mock_grpc_call):
    """Test gRPC error when retries are disabled."""
    client.retries_enabled = False
    mock_error = grpc.RpcError("gRPC failure")
    mock_error.code = lambda: grpc.StatusCode.UNKNOWN # Add code() method
    mock_grpc_call.side_effect = mock_error
    
    with pytest.raises(VortexApiError, match="Failed to test_op_fail"):
        client._execute_with_retry(mock_grpc_call, "test_op_fail")
    
    mock_grpc_call.assert_called_once()

def test_retry_successful_on_first_attempt(client, mock_grpc_call):
    """Test successful call on the first attempt when retries are enabled."""
    client.retries_enabled = True
    mock_grpc_call.return_value = "success_first_try"
    
    result = client._execute_with_retry(mock_grpc_call, "test_op_first")
    
    assert result == "success_first_try"
    mock_grpc_call.assert_called_once()

@patch('time.sleep', return_value=None) # Mock time.sleep
def test_retry_succeeds_after_one_retryable_error(mock_sleep, client, mock_grpc_call):
    """Test successful call after one retryable gRPC error."""
    client.retries_enabled = True
    client.max_retries = 1
    client.initial_backoff_ms = 100
    client.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    # Simulate UNAVAILABLE error then success
    mock_unavailable_error = grpc.RpcError("Unavailable")
    mock_unavailable_error.code = lambda: grpc.StatusCode.UNAVAILABLE # Mock the code() method
    
    mock_grpc_call.side_effect = [mock_unavailable_error, "success_after_retry"]
    
    result = client._execute_with_retry(mock_grpc_call, "test_op_retry_once")
    
    assert result == "success_after_retry"
    assert mock_grpc_call.call_count == 2
    mock_sleep.assert_called_once() # Check that sleep was called
    # We can also check the sleep duration if jitter is disabled or mocked
    # For now, just checking it was called is sufficient.

@patch('time.sleep', return_value=None)
@patch('random.uniform', return_value=0.05) # Mock jitter to be predictable (e.g., +5%)
def test_retry_exhausted_all_attempts(mock_uniform, mock_sleep, client, mock_grpc_call):
    """Test that VortexApiError is raised after all retries are exhausted."""
    client.retries_enabled = True
    client.max_retries = 2
    client.initial_backoff_ms = 50
    client.backoff_multiplier = 2.0
    client.retry_jitter = True # Enable jitter
    client.retryable_status_codes = [grpc.StatusCode.RESOURCE_EXHAUSTED]

    mock_exhausted_error = grpc.RpcError("Resource exhausted")
    mock_exhausted_error.code = lambda: grpc.StatusCode.RESOURCE_EXHAUSTED
    
    mock_grpc_call.side_effect = [mock_exhausted_error] * (client.max_retries + 1) # Fail all attempts
    
    with pytest.raises(VortexApiError, match=r"Failed to test_op_exhausted \(Status Code: RESOURCE_EXHAUSTED\) Details: Resource exhausted"):
        client._execute_with_retry(mock_grpc_call, "test_op_exhausted")
        
    assert mock_grpc_call.call_count == client.max_retries + 1
    assert mock_sleep.call_count == client.max_retries
    
    # Check sleep durations with jitter
    expected_sleep_1 = (50 * (1 + 0.05)) / 1000.0 # 50ms + 5% jitter
    expected_sleep_2 = (50 * 2.0 * (1 + 0.05)) / 1000.0 # 100ms + 5% jitter
    
    assert mock_sleep.call_args_list[0][0][0] == pytest.approx(expected_sleep_1)
    assert mock_sleep.call_args_list[1][0][0] == pytest.approx(expected_sleep_2)
    mock_uniform.assert_any_call(-0.1, 0.1) # Ensure random.uniform was called for jitter

def test_retry_non_retryable_grpc_error(client, mock_grpc_call):
    """Test that non-retryable gRPC errors are not retried."""
    client.retries_enabled = True
    client.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    mock_invalid_arg_error = grpc.RpcError("Invalid argument")
    mock_invalid_arg_error.code = lambda: grpc.StatusCode.INVALID_ARGUMENT # Non-retryable
    
    mock_grpc_call.side_effect = mock_invalid_arg_error
    
    with pytest.raises(VortexApiError, match="Failed to test_op_non_retryable"):
        client._execute_with_retry(mock_grpc_call, "test_op_non_retryable")
        
    mock_grpc_call.assert_called_once() # Should only be called once

def test_retry_unexpected_exception(client, mock_grpc_call):
    """Test handling of unexpected non-gRPC exceptions during a call."""
    client.retries_enabled = True
    mock_grpc_call.side_effect = ValueError("Unexpected Python error")
    
    with pytest.raises(VortexException, match="An unexpected error occurred during test_op_unexpected_py_err: Unexpected Python error"):
        client._execute_with_retry(mock_grpc_call, "test_op_unexpected_py_err")
        
    mock_grpc_call.assert_called_once()

@patch('time.sleep', return_value=None)
def test_retry_backoff_capping(mock_sleep, client, mock_grpc_call):
    """Test that backoff duration is capped by max_backoff_ms."""
    client.retries_enabled = True
    client.max_retries = 3
    client.initial_backoff_ms = 1000
    client.max_backoff_ms = 2500 # Cap
    client.backoff_multiplier = 2.0
    client.retry_jitter = False # Disable jitter for predictable sleep times
    client.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    mock_error = grpc.RpcError("Unavailable")
    mock_error.code = lambda: grpc.StatusCode.UNAVAILABLE
    
    # Fail 3 times, succeed on 4th
    mock_grpc_call.side_effect = [mock_error, mock_error, mock_error, "success_capped_backoff"]
    
    client._execute_with_retry(mock_grpc_call, "test_op_capped")
    
    assert mock_grpc_call.call_count == 4
    assert mock_sleep.call_count == 3
    
    # Expected sleep durations (no jitter):
    # 1st retry: initial_backoff_ms = 1000ms
    # 2nd retry: 1000 * 2.0 = 2000ms
    # 3rd retry: 2000 * 2.0 = 4000ms, but capped at max_backoff_ms = 2500ms
    assert mock_sleep.call_args_list[0][0][0] == 1.0 # 1000ms
    assert mock_sleep.call_args_list[1][0][0] == 2.0 # 2000ms
    assert mock_sleep.call_args_list[2][0][0] == 2.5 # 2500ms (capped)

def test_retry_with_custom_retryable_codes(client_fixture_factory, mock_grpc_call):
    """Test retry logic with custom retryable status codes."""
    # Use a factory to create a client with specific retry config for this test
    custom_client = client_fixture_factory(
        retries_enabled=True,
        max_retries=1,
        retryable_status_codes=[grpc.StatusCode.INTERNAL, grpc.StatusCode.DATA_LOSS]
    )
    
    mock_internal_error = grpc.RpcError("Internal Server Error")
    mock_internal_error.code = lambda: grpc.StatusCode.INTERNAL
    
    mock_grpc_call.side_effect = [mock_internal_error, "success_custom_code"]
    
    with patch('time.sleep', return_value=None) as mock_sleep_custom:
        result = custom_client._execute_with_retry(mock_grpc_call, "test_op_custom_codes")
    
    assert result == "success_custom_code"
    assert mock_grpc_call.call_count == 2
    mock_sleep_custom.assert_called_once()

    # Now test that a default retryable code (UNAVAILABLE) is NOT retried
    mock_grpc_call.reset_mock()
    mock_unavailable_error = grpc.RpcError("Unavailable")
    mock_unavailable_error.code = lambda: grpc.StatusCode.UNAVAILABLE
    mock_grpc_call.side_effect = mock_unavailable_error

    with pytest.raises(VortexApiError, match="Failed to test_op_default_not_custom"):
        custom_client._execute_with_retry(mock_grpc_call, "test_op_default_not_custom")
    assert mock_grpc_call.call_count == 1


# Helper fixture to create a client with specific retry parameters for certain tests
@pytest.fixture
def client_fixture_factory(mocker, mock_grpc_channel, mock_collections_stub, mock_points_stub):
    def _factory(**retry_kwargs):
        # Ensure default mocks are in place
        mocker.patch('grpc.insecure_channel', return_value=mock_grpc_channel)
        mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_collections_stub)
        mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_points_stub)
        mocker.patch('vortex_sdk.client.VortexClient._connect', MagicMock())

        # Create client with specified retry_kwargs
        vc = VortexClient(host="mockhost_factory", port=12345, **retry_kwargs)
        vc._channel = mock_grpc_channel
        vc._collections_stub = mock_collections_stub
        vc._points_stub = mock_points_stub
        return vc
    return _factory

# TODO: Add more tests for edge cases, different parameter combinations, and error handling.
# For example, test the case where the last_exception is None in _execute_with_retry,
# though it seems hard to reach with current logic.
# Also, test interaction with actual client methods if retry logic affects their error handling.
# The current tests focus on _execute_with_retry directly.
