from . import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UpsertPointsRequest(_message.Message):
    __slots__ = ("collection_name", "points", "wait_flush")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FLUSH_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    points: _containers.RepeatedCompositeFieldContainer[_common_pb2.PointStruct]
    wait_flush: bool
    def __init__(self, collection_name: _Optional[str] = ..., points: _Optional[_Iterable[_Union[_common_pb2.PointStruct, _Mapping]]] = ..., wait_flush: bool = ...) -> None: ...

class UpsertPointsResponse(_message.Message):
    __slots__ = ("statuses", "overall_error")
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    OVERALL_ERROR_FIELD_NUMBER: _ClassVar[int]
    statuses: _containers.RepeatedCompositeFieldContainer[_common_pb2.PointOperationStatus]
    overall_error: str
    def __init__(self, statuses: _Optional[_Iterable[_Union[_common_pb2.PointOperationStatus, _Mapping]]] = ..., overall_error: _Optional[str] = ...) -> None: ...

class GetPointsRequest(_message.Message):
    __slots__ = ("collection_name", "ids", "with_payload", "with_vector")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WITH_VECTOR_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    ids: _containers.RepeatedScalarFieldContainer[str]
    with_payload: bool
    with_vector: bool
    def __init__(self, collection_name: _Optional[str] = ..., ids: _Optional[_Iterable[str]] = ..., with_payload: bool = ..., with_vector: bool = ...) -> None: ...

class GetPointsResponse(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[_common_pb2.PointStruct]
    def __init__(self, points: _Optional[_Iterable[_Union[_common_pb2.PointStruct, _Mapping]]] = ...) -> None: ...

class DeletePointsRequest(_message.Message):
    __slots__ = ("collection_name", "ids", "wait_flush")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    WAIT_FLUSH_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    ids: _containers.RepeatedScalarFieldContainer[str]
    wait_flush: bool
    def __init__(self, collection_name: _Optional[str] = ..., ids: _Optional[_Iterable[str]] = ..., wait_flush: bool = ...) -> None: ...

class DeletePointsResponse(_message.Message):
    __slots__ = ("statuses", "overall_error")
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    OVERALL_ERROR_FIELD_NUMBER: _ClassVar[int]
    statuses: _containers.RepeatedCompositeFieldContainer[_common_pb2.PointOperationStatus]
    overall_error: str
    def __init__(self, statuses: _Optional[_Iterable[_Union[_common_pb2.PointOperationStatus, _Mapping]]] = ..., overall_error: _Optional[str] = ...) -> None: ...

class SearchPointsRequest(_message.Message):
    __slots__ = ("collection_name", "query_vector", "k_limit", "filter", "with_payload", "with_vector", "params")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    K_LIMIT_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WITH_VECTOR_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    query_vector: _common_pb2.Vector
    k_limit: int
    filter: _common_pb2.Filter
    with_payload: bool
    with_vector: bool
    params: _common_pb2.SearchParams
    def __init__(self, collection_name: _Optional[str] = ..., query_vector: _Optional[_Union[_common_pb2.Vector, _Mapping]] = ..., k_limit: _Optional[int] = ..., filter: _Optional[_Union[_common_pb2.Filter, _Mapping]] = ..., with_payload: bool = ..., with_vector: bool = ..., params: _Optional[_Union[_common_pb2.SearchParams, _Mapping]] = ...) -> None: ...

class SearchPointsResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[_common_pb2.ScoredPoint]
    def __init__(self, results: _Optional[_Iterable[_Union[_common_pb2.ScoredPoint, _Mapping]]] = ...) -> None: ...
