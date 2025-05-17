"""
Unit tests for the AsyncVortexClient.
"""
import pytest
import grpc.aio # type: ignore
from unittest.mock import MagicMock, AsyncMock, patch

from vortex_sdk import AsyncVortexClient, models
from vortex_sdk.exceptions import VortexApiError, VortexConnectionError, VortexException, VortexClientConfigurationError
from vortex_sdk._grpc.vortex.api.v1 import collections_service_pb2
from vortex_sdk._grpc.vortex.api.v1 import points_service_pb2
from vortex_sdk._grpc.vortex.api.v1 import common_pb2

@pytest.fixture
def mock_aio_grpc_channel():
    # For grpc.aio.Channel, methods like close are coroutines
    channel = MagicMock(spec=grpc.aio.Channel)
    channel.close = AsyncMock() 
    return channel

@pytest.fixture
def mock_aio_collections_stub():
    # For stubs used with grpc.aio.Channel, RPC methods are coroutines
    stub = MagicMock()
    stub.CreateCollection = AsyncMock()
    stub.GetCollectionInfo = AsyncMock()
    stub.ListCollections = AsyncMock()
    stub.DeleteCollection = AsyncMock()
    return stub

@pytest.fixture
def mock_aio_points_stub():
    stub = MagicMock()
    stub.UpsertPoints = AsyncMock()
    stub.GetPoints = AsyncMock()
    stub.DeletePoints = AsyncMock()
    stub.SearchPoints = AsyncMock()
    return stub

@pytest.fixture
async def async_client(mocker, mock_aio_grpc_channel, mock_aio_collections_stub, mock_aio_points_stub):
    """Fixture to create an AsyncVortexClient with mocked gRPC stubs."""
    mocker.patch('grpc.aio.insecure_channel', return_value=mock_aio_grpc_channel)
    
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)
    
    # Mock the connect method to prevent real connection attempts during test setup
    # and to allow manual assignment of stubs.
    mocker.patch('vortex_sdk.client.AsyncVortexClient.connect', AsyncMock())

    aclient = AsyncVortexClient(host="mockhost", port=12345)
    # Manually assign stubs as connect is mocked
    aclient._channel = mock_aio_grpc_channel
    aclient._collections_stub = mock_aio_collections_stub
    aclient._points_stub = mock_aio_points_stub
    
    # We don't call await aclient.connect() here because the test methods
    # themselves will trigger it if stubs are None, or we can call it explicitly.
    # For tests that assume connection, we can call it.
    # Let's assume tests will ensure connection or it's handled by __aenter__ if used.
    return aclient

# --- Async Connection Tests ---

@pytest.mark.asyncio
async def test_async_client_connect_insecure(mocker, mock_aio_collections_stub, mock_aio_points_stub):
    """Test insecure async channel creation."""
    mock_insecure_channel = mocker.patch('grpc.aio.insecure_channel', return_value=MagicMock(spec=grpc.aio.Channel))
    # Ensure the channel's close method is an AsyncMock
    mock_insecure_channel.return_value.close = AsyncMock()

    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)

    options = [("grpc.lb_policy_name", "pick_first")]
    aclient = AsyncVortexClient(host="test_host_async", port=123, secure=False, grpc_options=options)
    await aclient.connect() # Explicitly connect
    
    mock_insecure_channel.assert_called_once_with("test_host_async:123", options=options)
    assert aclient._channel == mock_insecure_channel.return_value
    assert aclient._collections_stub is not None
    assert aclient._points_stub is not None
    await aclient.close()

@pytest.mark.asyncio
async def test_async_client_connect_secure_server_auth(mocker, mock_aio_collections_stub, mock_aio_points_stub):
    """Test secure async channel creation with server authentication."""
    mock_secure_channel = mocker.patch('grpc.aio.secure_channel', return_value=MagicMock(spec=grpc.aio.Channel))
    mock_secure_channel.return_value.close = AsyncMock()
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials)) # Corrected path
    
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)

    root_certs_data = b"test_root_certs_async"
    options = [("grpc.ssl_target_name_override", "server.name.async")]
    
    aclient = AsyncVortexClient(
        host="secure_host_async", port=443, secure=True, 
        root_certs=root_certs_data, grpc_options=options
    )
    await aclient.connect()
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=root_certs_data,
        private_key=None,
        certificate_chain=None
    )
    mock_secure_channel.assert_called_once_with(
        "secure_host_async:443", mock_ssl_creds.return_value, options=options
    )
    assert aclient._channel == mock_secure_channel.return_value
    await aclient.close()

@pytest.mark.asyncio
async def test_async_client_connect_secure_mtls(mocker, mock_aio_collections_stub, mock_aio_points_stub):
    """Test secure async channel creation with mutual TLS."""
    mock_secure_channel = mocker.patch('grpc.aio.secure_channel', return_value=MagicMock(spec=grpc.aio.Channel))
    mock_secure_channel.return_value.close = AsyncMock()
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials)) # Corrected path
    
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)

    root_certs_data = b"test_root_certs_mtls_async"
    private_key_data = b"test_private_key_mtls_async"
    cert_chain_data = b"test_cert_chain_mtls_async"
    
    aclient = AsyncVortexClient(
        host="mtls_host_async", port=443, secure=True, 
        root_certs=root_certs_data, private_key=private_key_data, certificate_chain=cert_chain_data
    )
    await aclient.connect()
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=root_certs_data,
        private_key=private_key_data,
        certificate_chain=cert_chain_data
    )
    mock_secure_channel.assert_called_once_with(
        "mtls_host_async:443", mock_ssl_creds.return_value, options=None
    )
    await aclient.close()

@pytest.mark.asyncio
async def test_async_client_connect_secure_system_ca(mocker, mock_aio_collections_stub, mock_aio_points_stub):
    """Test secure async channel using system CAs."""
    mock_secure_channel = mocker.patch('grpc.aio.secure_channel', return_value=MagicMock(spec=grpc.aio.Channel))
    mock_secure_channel.return_value.close = AsyncMock()
    mock_ssl_creds = mocker.patch('grpc.ssl_channel_credentials', return_value=MagicMock(spec=grpc.ChannelCredentials)) # Corrected path
    
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)
    
    aclient = AsyncVortexClient(host="systemca_host_async", port=443, secure=True)
    await aclient.connect()
    
    mock_ssl_creds.assert_called_once_with(
        root_certificates=None, private_key=None, certificate_chain=None
    )
    mock_secure_channel.assert_called_once_with(
        "systemca_host_async:443", mock_ssl_creds.return_value, options=None
    )
    await aclient.close()


@pytest.mark.asyncio
async def test_async_client_context_manager(mocker, mock_aio_grpc_channel, mock_aio_collections_stub, mock_aio_points_stub):
    """Test the async client's async context manager."""
    mocker.patch('grpc.aio.insecure_channel', return_value=mock_aio_grpc_channel)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
    mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)

    connect_spy = mocker.spy(AsyncVortexClient, 'connect')
    close_spy = mocker.spy(AsyncVortexClient, 'close')

    async with AsyncVortexClient(host="mockhost", port=12345) as aclient:
        assert aclient._channel is not None
        assert aclient._collections_stub is not None
        assert aclient._points_stub is not None
        connect_spy.assert_called_once_with(aclient) # connect is called by __aenter__
    
    close_spy.assert_called_once_with(aclient) # close is called by __aexit__
    assert mock_aio_grpc_channel.close.called # Ensure the underlying channel's close was awaited


# --- Async CollectionsService Method Tests ---

@pytest.mark.asyncio
async def test_async_create_collection_success(async_client, mock_aio_collections_stub):
    mock_aio_collections_stub.CreateCollection.return_value = collections_service_pb2.CreateCollectionResponse()
    
    client_instance = await async_client # Await the fixture
    await client_instance.create_collection(
        collection_name="test_coll_async",
        vector_dimensions=128,
        distance_metric=models.DistanceMetric.COSINE,
        hnsw_config=models.HnswConfigParams(m=16, ef_construction=100, ef_search=50, ml=0.5, vector_dim=128, m_max0=32)
    )
    mock_aio_collections_stub.CreateCollection.assert_awaited_once()

@pytest.mark.asyncio
async def test_async_create_collection_api_error(async_client, mock_aio_collections_stub):
    # Create a mock AioRpcError object
    mock_error = grpc.aio.AioRpcError(grpc.StatusCode.INTERNAL, initial_metadata=None, trailing_metadata=None, details="Mock gRPC async error")
    mock_aio_collections_stub.CreateCollection.side_effect = mock_error
    
    client_instance = await async_client # Await the fixture
    with pytest.raises(VortexApiError, match="Failed to create collection 'test_coll_async_fail'"):
        await client_instance.create_collection(
            collection_name="test_coll_async_fail",
            vector_dimensions=128,
            distance_metric=models.DistanceMetric.EUCLIDEAN_L2
        )

@pytest.mark.asyncio
async def test_async_get_collection_info_success(async_client, mock_aio_collections_stub):
    mock_response = collections_service_pb2.GetCollectionInfoResponse(
        collection_name="info_coll_async",
        status=collections_service_pb2.CollectionStatus.GREEN,
        vector_count=100,
        config=common_pb2.HnswConfigParams(m=8, ef_construction=50, ef_search=20, ml=0.3, vector_dim=64, m_max0=16),
        distance_metric=common_pb2.DistanceMetric.EUCLIDEAN_L2
    )
    mock_aio_collections_stub.GetCollectionInfo.return_value = mock_response
    
    client_instance = await async_client # Await the fixture
    info = await client_instance.get_collection_info("info_coll_async")
    
    assert info.collection_name == "info_coll_async"
    assert info.status == models.CollectionStatus.GREEN
    mock_aio_collections_stub.GetCollectionInfo.assert_awaited_once()

# --- Async PointsService Method Tests ---

@pytest.mark.asyncio
async def test_async_upsert_points_success(async_client, mock_aio_points_stub):
    points_to_upsert = [models.PointStruct(id="ap1", vector=models.Vector(elements=[0.1, 0.2]))]
    mock_response = points_service_pb2.UpsertPointsResponse(
        statuses=[common_pb2.PointOperationStatus(point_id="ap1", status_code=common_pb2.StatusCode.OK)]
    )
    mock_aio_points_stub.UpsertPoints.return_value = mock_response

    client_instance = await async_client # Await the fixture
    statuses = await client_instance.upsert_points(collection_name="upsert_coll_async", points=points_to_upsert)

    assert len(statuses) == 1
    assert statuses[0].point_id == "ap1"
    mock_aio_points_stub.UpsertPoints.assert_awaited_once()

@pytest.mark.asyncio
async def test_async_search_points_success(async_client, mock_aio_points_stub):
    query_vec = models.Vector(elements=[0.5, 0.6])
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[common_pb2.ScoredPoint(id="asp1", score=0.9, vector=common_pb2.Vector(elements=[0.51, 0.61]))]
    )
    mock_aio_points_stub.SearchPoints.return_value = mock_response

    client_instance = await async_client # Await the fixture
    results = await client_instance.search_points(
        collection_name="search_coll_async", 
        query_vector=query_vec, 
        k_limit=5,
        with_vector=True
    )
    assert len(results) == 1
    assert results[0].id == "asp1"
    assert results[0].score == pytest.approx(0.9)
    mock_aio_points_stub.SearchPoints.assert_awaited_once()
    call_args = mock_aio_points_stub.SearchPoints.call_args[0][0]
    assert not call_args.HasField("params")


@pytest.mark.asyncio
async def test_async_search_points_with_search_params(async_client, mock_aio_points_stub):
    """Test async point search with SearchParams."""
    query_vec = models.Vector(elements=[0.7, 0.8])
    search_p = models.SearchParams(ef_search=150)
    
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[common_pb2.ScoredPoint(id="asp_ef", score=0.95)]
    )
    mock_aio_points_stub.SearchPoints.return_value = mock_response

    client_instance = await async_client
    results = await client_instance.search_points(
        collection_name="search_params_coll_async",
        query_vector=query_vec,
        k_limit=3,
        search_params=search_p
    )
    assert len(results) == 1
    mock_aio_points_stub.SearchPoints.assert_awaited_once()
    call_args = mock_aio_points_stub.SearchPoints.call_args[0][0]
    assert call_args.collection_name == "search_params_coll_async"
    assert call_args.HasField("params")
    assert call_args.params.ef_search == 150

@pytest.mark.asyncio
async def test_async_search_points_with_empty_search_params(async_client, mock_aio_points_stub):
    """Test async point search with SearchParams that results in None after conversion."""
    query_vec = models.Vector(elements=[0.7, 0.8])
    search_p_empty = models.SearchParams() # ef_search is None
    
    mock_response = points_service_pb2.SearchPointsResponse(
        results=[common_pb2.ScoredPoint(id="asp_empty_ef", score=0.93)]
    )
    mock_aio_points_stub.SearchPoints.return_value = mock_response

    client_instance = await async_client
    await client_instance.search_points(
        collection_name="search_empty_params_coll_async",
        query_vector=query_vec,
        k_limit=2,
        search_params=search_p_empty
    )
    mock_aio_points_stub.SearchPoints.assert_awaited_once()
    call_args = mock_aio_points_stub.SearchPoints.call_args[0][0]
    assert not call_args.HasField("params")


# TODO: Add more async tests for list_collections, delete_collection, get_points, delete_points
# TODO: Add tests for error handling, connection logic, and other edge cases for AsyncVortexClient

# --- Async Retry Logic Tests (_execute_with_retry_async) ---

@pytest.fixture
def mock_async_grpc_call():
    return AsyncMock() # For async methods

@pytest.mark.asyncio
async def test_async_retry_disabled_successful_call(async_client, mock_async_grpc_call):
    """Test successful async call when retries are disabled."""
    client_instance = await async_client
    client_instance.retries_enabled = False
    mock_async_grpc_call.return_value = "async_success"
    
    result = await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op", "arg1", kwarg1="kwval1")
    
    assert result == "async_success"
    mock_async_grpc_call.assert_awaited_once_with("arg1", kwarg1="kwval1")

@pytest.mark.asyncio
async def test_async_retry_disabled_grpc_error(async_client, mock_async_grpc_call):
    """Test async gRPC error when retries are disabled."""
    client_instance = await async_client
    client_instance.retries_enabled = False
    # Use grpc.aio.AioRpcError for async client
    mock_error = grpc.aio.AioRpcError(grpc.StatusCode.INTERNAL, initial_metadata=None, trailing_metadata=None, details="Async gRPC failure")
    mock_async_grpc_call.side_effect = mock_error
    
    with pytest.raises(VortexApiError, match="Failed to test_async_op_fail"):
        await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_fail")
    
    mock_async_grpc_call.assert_awaited_once()

@pytest.mark.asyncio
async def test_async_retry_successful_on_first_attempt(async_client, mock_async_grpc_call):
    """Test successful async call on the first attempt when retries are enabled."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    mock_async_grpc_call.return_value = "async_success_first_try"
    
    result = await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_first")
    
    assert result == "async_success_first_try"
    mock_async_grpc_call.assert_awaited_once()

@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock) # Mock asyncio.sleep
async def test_async_retry_succeeds_after_one_retryable_error(mock_async_sleep, async_client, mock_async_grpc_call):
    """Test successful async call after one retryable gRPC error."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    client_instance.max_retries = 1
    client_instance.initial_backoff_ms = 100
    client_instance.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    mock_unavailable_error = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Async Unavailable")
    
    mock_async_grpc_call.side_effect = [mock_unavailable_error, "async_success_after_retry"]
    
    result = await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_retry_once")
    
    assert result == "async_success_after_retry"
    assert mock_async_grpc_call.await_count == 2
    mock_async_sleep.assert_awaited_once()

@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock)
@patch('random.uniform', return_value=0.05) # Mock jitter
async def test_async_retry_exhausted_all_attempts(mock_uniform, mock_async_sleep, async_client, mock_async_grpc_call):
    """Test VortexApiError after all async retries are exhausted."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    client_instance.max_retries = 2
    client_instance.initial_backoff_ms = 50
    client_instance.backoff_multiplier = 2.0
    client_instance.retry_jitter = True
    client_instance.retryable_status_codes = [grpc.StatusCode.RESOURCE_EXHAUSTED]

    mock_exhausted_error = grpc.aio.AioRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, initial_metadata=None, trailing_metadata=None, details="Async Resource exhausted")
    
    mock_async_grpc_call.side_effect = [mock_exhausted_error] * (client_instance.max_retries + 1)
    
    with pytest.raises(VortexApiError, match=r"Failed to test_async_op_exhausted \(Status Code: RESOURCE_EXHAUSTED\) Details: Async Resource exhausted"):
        await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_exhausted")
        
    assert mock_async_grpc_call.await_count == client_instance.max_retries + 1
    assert mock_async_sleep.await_count == client_instance.max_retries
    
    expected_sleep_1 = (50 * (1 + 0.05)) / 1000.0
    expected_sleep_2 = (50 * 2.0 * (1 + 0.05)) / 1000.0
    
    assert mock_async_sleep.call_args_list[0][0][0] == pytest.approx(expected_sleep_1)
    assert mock_async_sleep.call_args_list[1][0][0] == pytest.approx(expected_sleep_2)
    mock_uniform.assert_any_call(-0.1, 0.1)

@pytest.mark.asyncio
async def test_async_retry_non_retryable_grpc_error(async_client, mock_async_grpc_call):
    """Test non-retryable async gRPC errors are not retried."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    client_instance.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    mock_invalid_arg_error = grpc.aio.AioRpcError(grpc.StatusCode.INVALID_ARGUMENT, initial_metadata=None, trailing_metadata=None, details="Async Invalid argument")
    
    mock_async_grpc_call.side_effect = mock_invalid_arg_error
    
    with pytest.raises(VortexApiError, match="Failed to test_async_op_non_retryable"):
        await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_non_retryable")
        
    mock_async_grpc_call.assert_awaited_once()

@pytest.mark.asyncio
async def test_async_retry_unexpected_exception(async_client, mock_async_grpc_call):
    """Test handling of unexpected non-gRPC exceptions during an async call."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    mock_async_grpc_call.side_effect = ValueError("Unexpected Python error in async")
    
    with pytest.raises(VortexException, match="An unexpected error occurred during test_async_op_unexpected_py_err: Unexpected Python error in async"):
        await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_unexpected_py_err")
        
    mock_async_grpc_call.assert_awaited_once()

@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock)
async def test_async_retry_backoff_capping(mock_async_sleep, async_client, mock_async_grpc_call):
    """Test async backoff duration is capped by max_backoff_ms."""
    client_instance = await async_client
    client_instance.retries_enabled = True
    client_instance.max_retries = 3
    client_instance.initial_backoff_ms = 1000
    client_instance.max_backoff_ms = 2500 # Cap
    client_instance.backoff_multiplier = 2.0
    client_instance.retry_jitter = False # Disable jitter
    client_instance.retryable_status_codes = [grpc.StatusCode.UNAVAILABLE]

    mock_error = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Async Unavailable Capped")
    
    mock_async_grpc_call.side_effect = [mock_error, mock_error, mock_error, "async_success_capped_backoff"]
    
    await client_instance._execute_with_retry_async(mock_async_grpc_call, "test_async_op_capped")
    
    assert mock_async_grpc_call.await_count == 4
    assert mock_async_sleep.await_count == 3
    
    assert mock_async_sleep.call_args_list[0][0][0] == 1.0
    assert mock_async_sleep.call_args_list[1][0][0] == 2.0
    assert mock_async_sleep.call_args_list[2][0][0] == 2.5

@pytest.mark.asyncio
async def test_async_retry_with_custom_retryable_codes(async_client_fixture_factory, mock_async_grpc_call):
    """Test async retry logic with custom retryable status codes."""
    factory_fn = async_client_fixture_factory # Get the factory function
    custom_async_client = await factory_fn( # Call and await the factory
        retries_enabled=True,
        max_retries=1,
        retryable_status_codes=[grpc.StatusCode.INTERNAL, grpc.StatusCode.DATA_LOSS]
    )
    
    mock_internal_error = grpc.aio.AioRpcError(grpc.StatusCode.INTERNAL, initial_metadata=None, trailing_metadata=None, details="Async Internal Error")
    
    mock_async_grpc_call.side_effect = [mock_internal_error, "async_success_custom_code"]
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_async_sleep_custom:
        result = await custom_async_client._execute_with_retry_async(mock_async_grpc_call, "test_async_op_custom_codes")
    
    assert result == "async_success_custom_code"
    assert mock_async_grpc_call.await_count == 2
    mock_async_sleep_custom.assert_awaited_once()

    mock_async_grpc_call.reset_mock() # Reset for next part of test
    mock_unavailable_error = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, initial_metadata=None, trailing_metadata=None, details="Async Unavailable Custom")
    mock_async_grpc_call.side_effect = mock_unavailable_error

    with pytest.raises(VortexApiError, match="Failed to test_async_op_default_not_custom"):
        await custom_async_client._execute_with_retry_async(mock_async_grpc_call, "test_async_op_default_not_custom")
    assert mock_async_grpc_call.await_count == 1


# Helper fixture to create an AsyncVortexClient with specific retry parameters
@pytest.fixture
def async_client_fixture_factory(mocker, mock_aio_grpc_channel, mock_aio_collections_stub, mock_aio_points_stub): # Not async
    async def _factory(**retry_kwargs): # Inner factory is async
        mocker.patch('grpc.aio.insecure_channel', return_value=mock_aio_grpc_channel)
        mocker.patch('vortex_sdk._grpc.vortex.api.v1.collections_service_pb2_grpc.CollectionsServiceStub', return_value=mock_aio_collections_stub)
        mocker.patch('vortex_sdk._grpc.vortex.api.v1.points_service_pb2_grpc.PointsServiceStub', return_value=mock_aio_points_stub)
        mocker.patch('vortex_sdk.client.AsyncVortexClient.connect', AsyncMock())

        aclient = AsyncVortexClient(host="mockhost_async_factory", port=12345, **retry_kwargs)
        aclient._channel = mock_aio_grpc_channel
        aclient._collections_stub = mock_aio_collections_stub
        aclient._points_stub = mock_aio_points_stub
        # No await aclient.connect() here, assume it's handled by test or context manager
        return aclient
    return _factory
