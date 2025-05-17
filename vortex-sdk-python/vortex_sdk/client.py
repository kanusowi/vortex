"""
Main client for interacting with the Vortex Vector Database.
"""
import time
import random
import asyncio # Added for async sleep
from typing import Optional, Union, List, Dict, Any, Tuple, Callable, TypeVar, Awaitable
import grpc # type: ignore
import grpc.aio # For async client

# Generated gRPC stubs
from ._grpc.vortex.api.v1 import collections_service_pb2_grpc
from ._grpc.vortex.api.v1 import points_service_pb2_grpc
from ._grpc.vortex.api.v1 import common_pb2 # For common types
from ._grpc.vortex.api.v1 import collections_service_pb2 # For collections request/response types
from ._grpc.vortex.api.v1 import points_service_pb2 # For points request/response types

# Pydantic models
from . import models
from . import conversions

# Custom exceptions
from .exceptions import VortexConnectionError, VortexApiError, VortexClientConfigurationError, VortexException
# VortexApiException was removed as it's not defined in exceptions.py

class VortexClient:
    """
    The main synchronous client for interacting with a Vortex server.
    """
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051, # Default gRPC port for Vortex
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        secure: bool = False,
        root_certs: Optional[bytes] = None,
        private_key: Optional[bytes] = None,
        certificate_chain: Optional[bytes] = None,
        grpc_options: Optional[List[Tuple[str, Any]]] = None,
        retries_enabled: bool = True,
        max_retries: int = 3,
        initial_backoff_ms: int = 200,
        max_backoff_ms: int = 5000,
        backoff_multiplier: float = 1.5,
        retry_jitter: bool = True,
        retryable_status_codes: Optional[List[grpc.StatusCode]] = None,
    ):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.timeout = timeout
        self.secure = secure
        self.root_certs = root_certs
        self.private_key = private_key
        self.certificate_chain = certificate_chain
        self.grpc_options = grpc_options

        # Retry configuration
        self.retries_enabled = retries_enabled
        self.max_retries = max_retries
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.retry_jitter = retry_jitter
        self.retryable_status_codes = retryable_status_codes or [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        ]
        
        self._channel: Optional[grpc.Channel] = None
        self._collections_stub: Optional[collections_service_pb2_grpc.CollectionsServiceStub] = None
        self._points_stub: Optional[points_service_pb2_grpc.PointsServiceStub] = None

        self._connect()

    def _connect(self) -> None:
        """Establishes the gRPC connection."""
        if self._channel:
            self._channel.close()

        target = f"{self.host}:{self.port}"
        try:
            if self.secure:
                if self.root_certs is None and (self.private_key is not None or self.certificate_chain is not None):
                    pass 

                credentials = grpc.ssl_channel_credentials(
                    root_certificates=self.root_certs,
                    private_key=self.private_key,
                    certificate_chain=self.certificate_chain
                )
                self._channel = grpc.secure_channel(target, credentials, options=self.grpc_options)
            else:
                self._channel = grpc.insecure_channel(target, options=self.grpc_options)
            
            self._collections_stub = collections_service_pb2_grpc.CollectionsServiceStub(self._channel)
            self._points_stub = points_service_pb2_grpc.PointsServiceStub(self._channel)
            
        except grpc.RpcError as e:
            raise VortexConnectionError(f"Failed to connect to Vortex at {target}: {e}")
        except Exception as e: 
            raise VortexConnectionError(f"An unexpected error occurred while connecting to {target}: {e}")

    def close(self) -> None:
        """Closes the gRPC connection."""
        if self._channel:
            self._channel.close()
            self._channel = None
            self._collections_stub = None
            self._points_stub = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # --- Retry Helper ---
    def _execute_with_retry(self, grpc_call: Callable, operation_name: str, *args, **kwargs):
        """
        Executes a gRPC call with retry logic for specific error codes.
        """
        if not self.retries_enabled:
            try:
                return grpc_call(*args, **kwargs)
            except grpc.RpcError as e:
                raise VortexApiError(f"Failed to {operation_name}", grpc_error=e)
            except VortexApiError: # Changed from VortexApiException
                raise
            except Exception as e: 
                raise VortexException(f"An unexpected error occurred during {operation_name}: {e}")

        last_exception = None
        current_backoff_ms = self.initial_backoff_ms

        for attempt in range(self.max_retries + 1):
            try:
                return grpc_call(*args, **kwargs)
            except grpc.RpcError as e:
                last_exception = e
                if hasattr(e, 'code') and callable(e.code) and e.code() in self.retryable_status_codes:
                    if attempt < self.max_retries:
                        sleep_duration_ms = current_backoff_ms
                        if self.retry_jitter:
                            sleep_duration_ms *= (1 + random.uniform(-0.1, 0.1))
                        
                        time.sleep(sleep_duration_ms / 1000.0)
                        
                        current_backoff_ms = min(self.max_backoff_ms, current_backoff_ms * self.backoff_multiplier)
                        continue 
                raise VortexApiError(f"Failed to {operation_name}", grpc_error=e) 
            except VortexApiError: # Changed from VortexApiException
                raise
            except Exception as e: 
                raise VortexException(f"An unexpected error occurred during {operation_name}: {e}")
        
        if last_exception:
            raise VortexApiError(f"Failed to {operation_name} after {self.max_retries} retries", grpc_error=last_exception)
        
        raise VortexException(f"Failed to {operation_name} after all retries, but no gRPC exception was captured.")


    # --- Collection Methods ---
    def create_collection(
        self,
        collection_name: str,
        vector_dimensions: int,
        distance_metric: models.DistanceMetric,
        hnsw_config: Optional[models.HnswConfigParams] = None,
    ) -> None:
        if not self._collections_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            grpc_distance_metric = conversions.pydantic_to_grpc_distance_metric(distance_metric)
            
            request_args = {
                "collection_name": collection_name,
                "vector_dimensions": vector_dimensions,
                "distance_metric": grpc_distance_metric,
            }
            if hnsw_config:
                request_args["hnsw_config"] = conversions.pydantic_to_grpc_hnsw_config(hnsw_config)
            
            request = collections_service_pb2.CreateCollectionRequest(**request_args)
            
            self._execute_with_retry(
                self._collections_stub.CreateCollection,
                f"create collection '{collection_name}'",
                request,
                timeout=self.timeout
            )
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)): 
                 raise VortexException(f"An unexpected pre-call error occurred while creating collection '{collection_name}': {e}")
            else:
                raise

    def get_collection_info(self, collection_name: str) -> models.CollectionInfo:
        if not self._collections_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            request = collections_service_pb2.GetCollectionInfoRequest(collection_name=collection_name)
            response = self._execute_with_retry(
                self._collections_stub.GetCollectionInfo,
                f"get collection info for '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return conversions.grpc_to_pydantic_collection_info(response)
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while getting collection info for '{collection_name}': {e}")
            else:
                raise

    def list_collections(self) -> List[models.CollectionDescription]:
        if not self._collections_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            request = collections_service_pb2.ListCollectionsRequest()
            response = self._execute_with_retry(
                self._collections_stub.ListCollections,
                "list collections",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_collection_description(desc) for desc in response.collections]
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while listing collections: {e}")
            else:
                raise

    def delete_collection(self, collection_name: str) -> None:
        if not self._collections_stub:
            raise VortexConnectionError("Client not connected.")
        
        try:
            request = collections_service_pb2.DeleteCollectionRequest(collection_name=collection_name)
            self._execute_with_retry(
                self._collections_stub.DeleteCollection,
                f"delete collection '{collection_name}'",
                request,
                timeout=self.timeout
            )
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while deleting collection '{collection_name}': {e}")
            else:
                raise

    # --- Point Methods ---
    def upsert_points(
        self,
        collection_name: str,
        points: List[models.PointStruct],
        wait_flush: Optional[bool] = None,
    ) -> List[models.PointOperationStatus]:
        if not self._points_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            grpc_points = [conversions.pydantic_to_grpc_point_struct(p) for p in points]
            request_args = {
                "collection_name": collection_name,
                "points": grpc_points,
            }
            if wait_flush is not None:
                request_args["wait_flush"] = wait_flush
            
            request = points_service_pb2.UpsertPointsRequest(**request_args)
            
            response = self._execute_with_retry(
                self._points_stub.UpsertPoints,
                f"upsert points in '{collection_name}'",
                request,
                timeout=self.timeout
            )
            
            if response.overall_error: 
                 raise VortexApiError(f"Overall error during upsert: {response.overall_error}", status_code=common_pb2.StatusCode.ERROR)

            return [conversions.grpc_to_pydantic_point_operation_status(s) for s in response.statuses]
        except VortexClientConfigurationError: 
            raise
        except VortexApiError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while upserting points in '{collection_name}': {e}")
            else:
                raise

    def get_points(
        self,
        collection_name: str,
        ids: List[str],
        with_payload: Optional[bool] = True, 
        with_vector: Optional[bool] = False, 
    ) -> List[models.PointStruct]:
        if not self._points_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            request_args = {
                "collection_name": collection_name,
                "ids": ids,
            }
            if with_payload is not None: 
                request_args["with_payload"] = with_payload
            if with_vector is not None:
                request_args["with_vector"] = with_vector

            request = points_service_pb2.GetPointsRequest(**request_args)
            response = self._execute_with_retry(
                self._points_stub.GetPoints,
                f"get points from '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_point_struct(p) for p in response.points]
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while getting points from '{collection_name}': {e}")
            else:
                raise

    def delete_points(
        self,
        collection_name: str,
        ids: List[str],
        wait_flush: Optional[bool] = None,
    ) -> List[models.PointOperationStatus]:
        if not self._points_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            request_args = {
                "collection_name": collection_name,
                "ids": ids,
            }
            if wait_flush is not None:
                request_args["wait_flush"] = wait_flush
            
            request = points_service_pb2.DeletePointsRequest(**request_args)
            response = self._execute_with_retry(
                self._points_stub.DeletePoints,
                f"delete points from '{collection_name}'",
                request,
                timeout=self.timeout
            )

            if response.overall_error:
                 raise VortexApiError(f"Overall error during delete: {response.overall_error}", status_code=common_pb2.StatusCode.ERROR)

            return [conversions.grpc_to_pydantic_point_operation_status(s) for s in response.statuses]
        except VortexClientConfigurationError: 
            raise
        except VortexApiError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while deleting points from '{collection_name}': {e}")
            else:
                raise

    def search_points(
        self,
        collection_name: str,
        query_vector: models.Vector,
        k_limit: int,
        filter: Optional[models.Filter] = None,
        with_payload: Optional[bool] = True, 
        with_vector: Optional[bool] = False, 
        search_params: Optional[models.SearchParams] = None,
    ) -> List[models.ScoredPoint]:
        if not self._points_stub:
            raise VortexConnectionError("Client not connected.")

        try:
            grpc_query_vector = conversions.pydantic_to_grpc_vector(query_vector)
            
            request_args: Dict[str, Any] = {
                "collection_name": collection_name,
                "query_vector": grpc_query_vector,
                "k_limit": k_limit,
            }
            if filter:
                grpc_filter = conversions.pydantic_to_grpc_filter(filter)
                if grpc_filter: 
                    request_args["filter"] = grpc_filter
            
            if with_payload is not None:
                request_args["with_payload"] = with_payload
            if with_vector is not None:
                request_args["with_vector"] = with_vector
            
            if search_params:
                grpc_search_params = conversions.pydantic_to_grpc_search_params(search_params)
                if grpc_search_params: 
                    request_args["params"] = grpc_search_params

            request = points_service_pb2.SearchPointsRequest(**request_args)
            response = self._execute_with_retry(
                self._points_stub.SearchPoints,
                f"search points in '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_scored_point(r) for r in response.results]
        except VortexClientConfigurationError: 
            raise
        except Exception as e: 
            if not isinstance(e, (VortexApiError, VortexException)):
                 raise VortexException(f"An unexpected pre-call error occurred while searching points in '{collection_name}': {e}")
            else:
                raise

class AsyncVortexClient:
    """
    The main asynchronous client for interacting with a Vortex server.
    """
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
        secure: bool = False,
        root_certs: Optional[bytes] = None,
        private_key: Optional[bytes] = None,
        certificate_chain: Optional[bytes] = None,
        grpc_options: Optional[List[Tuple[str, Any]]] = None,
        retries_enabled: bool = True,
        max_retries: int = 3,
        initial_backoff_ms: int = 200,
        max_backoff_ms: int = 5000,
        backoff_multiplier: float = 1.5,
        retry_jitter: bool = True,
        retryable_status_codes: Optional[List[grpc.StatusCode]] = None,
    ):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.timeout = timeout 
        self.secure = secure
        self.root_certs = root_certs
        self.private_key = private_key
        self.certificate_chain = certificate_chain
        self.grpc_options = grpc_options

        self.retries_enabled = retries_enabled
        self.max_retries = max_retries
        self.initial_backoff_ms = initial_backoff_ms
        self.max_backoff_ms = max_backoff_ms
        self.backoff_multiplier = backoff_multiplier
        self.retry_jitter = retry_jitter
        self.retryable_status_codes = retryable_status_codes or [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        ]
        
        self._channel: Optional[grpc.aio.Channel] = None
        self._collections_stub: Optional[collections_service_pb2_grpc.CollectionsServiceStub] = None
        self._points_stub: Optional[points_service_pb2_grpc.PointsServiceStub] = None
        
    async def connect(self) -> None:
        if self._channel:
            await self._channel.close()

        target = f"{self.host}:{self.port}"
        try:
            if self.secure:
                credentials = grpc.ssl_channel_credentials( 
                    root_certificates=self.root_certs,
                    private_key=self.private_key,
                    certificate_chain=self.certificate_chain
                )
                self._channel = grpc.aio.secure_channel(target, credentials, options=self.grpc_options)
            else:
                self._channel = grpc.aio.insecure_channel(target, options=self.grpc_options)
            
            self._collections_stub = collections_service_pb2_grpc.CollectionsServiceStub(self._channel)
            self._points_stub = points_service_pb2_grpc.PointsServiceStub(self._channel)
            
        except grpc.aio.AioRpcError as e: 
            raise VortexConnectionError(f"Failed to connect to Vortex at {target}: {e}")
        except Exception as e:
            raise VortexConnectionError(f"An unexpected error occurred while connecting to {target}: {e}")

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._collections_stub = None
            self._points_stub = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --- Async Retry Helper ---
    async def _execute_with_retry_async(self, async_grpc_call: Callable[..., Awaitable], operation_name: str, *args, **kwargs):
        """
        Executes an asynchronous gRPC call with retry logic.
        """
        if not self.retries_enabled:
            try:
                return await async_grpc_call(*args, **kwargs)
            except grpc.aio.AioRpcError as e:
                raise VortexApiError(f"Failed to {operation_name}", grpc_error=e)
            except VortexApiError: # Changed from VortexApiException
                raise
            except Exception as e:
                raise VortexException(f"An unexpected error occurred during {operation_name}: {e}")

        last_exception = None
        current_backoff_ms = self.initial_backoff_ms

        for attempt in range(self.max_retries + 1):
            try:
                return await async_grpc_call(*args, **kwargs)
            except grpc.aio.AioRpcError as e: 
                last_exception = e
                if hasattr(e, 'code') and callable(e.code) and e.code() in self.retryable_status_codes:
                    if attempt < self.max_retries:
                        sleep_duration_ms = current_backoff_ms
                        if self.retry_jitter:
                            sleep_duration_ms *= (1 + random.uniform(-0.1, 0.1))
                        
                        await asyncio.sleep(sleep_duration_ms / 1000.0) 
                        
                        current_backoff_ms = min(self.max_backoff_ms, current_backoff_ms * self.backoff_multiplier)
                        continue
                raise VortexApiError(f"Failed to {operation_name}", grpc_error=e) 
            except VortexApiError: # Changed from VortexApiException
                raise
            except Exception as e:
                raise VortexException(f"An unexpected error occurred during {operation_name}: {e}")
        
        if last_exception:
            raise VortexApiError(f"Failed to {operation_name} after {self.max_retries} retries", grpc_error=last_exception)
        
        raise VortexException(f"Failed to {operation_name} after all retries, but no gRPC exception was captured (async).")

    # --- Async Collection Methods ---
    async def create_collection(
        self,
        collection_name: str,
        vector_dimensions: int,
        distance_metric: models.DistanceMetric,
        hnsw_config: Optional[models.HnswConfigParams] = None,
    ) -> None:
        if not self._collections_stub:
            await self.connect() 
            if not self._collections_stub: 
                 raise VortexConnectionError("Client not connected after connect attempt.")

        try:
            grpc_distance_metric = conversions.pydantic_to_grpc_distance_metric(distance_metric)
            request_args = {
                "collection_name": collection_name,
                "vector_dimensions": vector_dimensions,
                "distance_metric": grpc_distance_metric,
            }
            if hnsw_config:
                request_args["hnsw_config"] = conversions.pydantic_to_grpc_hnsw_config(hnsw_config)
            request = collections_service_pb2.CreateCollectionRequest(**request_args)
            
            await self._execute_with_retry_async(
                self._collections_stub.CreateCollection,
                f"create collection '{collection_name}'",
                request,
                timeout=self.timeout
            )
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while creating collection '{collection_name}': {e}")
            else:
                raise

    async def get_collection_info(self, collection_name: str) -> models.CollectionInfo:
        if not self._collections_stub:
            await self.connect()
            if not self._collections_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            request = collections_service_pb2.GetCollectionInfoRequest(collection_name=collection_name)
            response = await self._execute_with_retry_async(
                self._collections_stub.GetCollectionInfo,
                f"get collection info for '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return conversions.grpc_to_pydantic_collection_info(response)
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while getting collection info for '{collection_name}': {e}")
            else:
                raise

    async def list_collections(self) -> List[models.CollectionDescription]:
        if not self._collections_stub:
            await self.connect()
            if not self._collections_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            request = collections_service_pb2.ListCollectionsRequest()
            response = await self._execute_with_retry_async(
                self._collections_stub.ListCollections,
                "list collections",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_collection_description(desc) for desc in response.collections]
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while listing collections: {e}")
            else:
                raise

    async def delete_collection(self, collection_name: str) -> None:
        if not self._collections_stub:
            await self.connect()
            if not self._collections_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            request = collections_service_pb2.DeleteCollectionRequest(collection_name=collection_name)
            await self._execute_with_retry_async(
                self._collections_stub.DeleteCollection,
                f"delete collection '{collection_name}'",
                request,
                timeout=self.timeout
            )
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while deleting collection '{collection_name}': {e}")
            else:
                raise

    # --- Async Point Methods ---
    async def upsert_points(
        self,
        collection_name: str,
        points: List[models.PointStruct],
        wait_flush: Optional[bool] = None,
    ) -> List[models.PointOperationStatus]:
        if not self._points_stub:
            await self.connect()
            if not self._points_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            grpc_points = [conversions.pydantic_to_grpc_point_struct(p) for p in points]
            request_args = {
                "collection_name": collection_name,
                "points": grpc_points,
            }
            if wait_flush is not None:
                request_args["wait_flush"] = wait_flush
            request = points_service_pb2.UpsertPointsRequest(**request_args)
            response = await self._execute_with_retry_async(
                self._points_stub.UpsertPoints,
                f"upsert points in '{collection_name}'",
                request,
                timeout=self.timeout
            )
            if response.overall_error:
                 raise VortexApiError(f"Overall error during upsert: {response.overall_error}", status_code=common_pb2.StatusCode.ERROR)
            return [conversions.grpc_to_pydantic_point_operation_status(s) for s in response.statuses]
        except VortexClientConfigurationError:
            raise
        except VortexApiError: 
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while upserting points in '{collection_name}': {e}")
            else:
                raise

    async def get_points(
        self,
        collection_name: str,
        ids: List[str],
        with_payload: Optional[bool] = True,
        with_vector: Optional[bool] = False,
    ) -> List[models.PointStruct]:
        if not self._points_stub:
            await self.connect()
            if not self._points_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            request_args = {
                "collection_name": collection_name,
                "ids": ids,
            }
            if with_payload is not None:
                request_args["with_payload"] = with_payload
            if with_vector is not None:
                request_args["with_vector"] = with_vector
            request = points_service_pb2.GetPointsRequest(**request_args)
            response = await self._execute_with_retry_async(
                self._points_stub.GetPoints,
                f"get points from '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_point_struct(p) for p in response.points]
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while getting points from '{collection_name}': {e}")
            else:
                raise

    async def delete_points(
        self,
        collection_name: str,
        ids: List[str],
        wait_flush: Optional[bool] = None,
    ) -> List[models.PointOperationStatus]:
        if not self._points_stub:
            await self.connect()
            if not self._points_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            request_args = {
                "collection_name": collection_name,
                "ids": ids,
            }
            if wait_flush is not None:
                request_args["wait_flush"] = wait_flush
            request = points_service_pb2.DeletePointsRequest(**request_args)
            response = await self._execute_with_retry_async(
                self._points_stub.DeletePoints,
                f"delete points from '{collection_name}'",
                request,
                timeout=self.timeout
            )
            if response.overall_error:
                 raise VortexApiError(f"Overall error during delete: {response.overall_error}", status_code=common_pb2.StatusCode.ERROR)
            return [conversions.grpc_to_pydantic_point_operation_status(s) for s in response.statuses]
        except VortexClientConfigurationError:
            raise
        except VortexApiError: 
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while deleting points from '{collection_name}': {e}")
            else:
                raise

    async def search_points(
        self,
        collection_name: str,
        query_vector: models.Vector,
        k_limit: int,
        filter: Optional[models.Filter] = None,
        with_payload: Optional[bool] = True,
        with_vector: Optional[bool] = False,
        search_params: Optional[models.SearchParams] = None,
    ) -> List[models.ScoredPoint]:
        if not self._points_stub:
            await self.connect()
            if not self._points_stub:
                 raise VortexConnectionError("Client not connected after connect attempt.")
        try:
            grpc_query_vector = conversions.pydantic_to_grpc_vector(query_vector)
            request_args: Dict[str, Any] = {
                "collection_name": collection_name,
                "query_vector": grpc_query_vector,
                "k_limit": k_limit,
            }
            if filter:
                grpc_filter = conversions.pydantic_to_grpc_filter(filter)
                if grpc_filter:
                    request_args["filter"] = grpc_filter
            if with_payload is not None:
                request_args["with_payload"] = with_payload
            if with_vector is not None:
                request_args["with_vector"] = with_vector
            
            if search_params:
                grpc_search_params = conversions.pydantic_to_grpc_search_params(search_params)
                if grpc_search_params:
                    request_args["params"] = grpc_search_params
            
            request = points_service_pb2.SearchPointsRequest(**request_args)
            response = await self._execute_with_retry_async(
                self._points_stub.SearchPoints,
                f"search points in '{collection_name}'",
                request,
                timeout=self.timeout
            )
            return [conversions.grpc_to_pydantic_scored_point(r) for r in response.results]
        except VortexClientConfigurationError:
            raise
        except Exception as e:
            if not isinstance(e, (VortexApiError, VortexException)):
                raise VortexException(f"An unexpected pre-call error occurred while searching points in '{collection_name}': {e}")
            else:
                raise
