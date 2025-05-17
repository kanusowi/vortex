from . import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CollectionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COLLECTION_STATUS_UNSPECIFIED: _ClassVar[CollectionStatus]
    GREEN: _ClassVar[CollectionStatus]
    YELLOW: _ClassVar[CollectionStatus]
    RED: _ClassVar[CollectionStatus]
    OPTIMIZING: _ClassVar[CollectionStatus]
    CREATING: _ClassVar[CollectionStatus]
COLLECTION_STATUS_UNSPECIFIED: CollectionStatus
GREEN: CollectionStatus
YELLOW: CollectionStatus
RED: CollectionStatus
OPTIMIZING: CollectionStatus
CREATING: CollectionStatus

class CreateCollectionRequest(_message.Message):
    __slots__ = ("collection_name", "vector_dimensions", "distance_metric", "hnsw_config")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    HNSW_CONFIG_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    vector_dimensions: int
    distance_metric: _common_pb2.DistanceMetric
    hnsw_config: _common_pb2.HnswConfigParams
    def __init__(self, collection_name: _Optional[str] = ..., vector_dimensions: _Optional[int] = ..., distance_metric: _Optional[_Union[_common_pb2.DistanceMetric, str]] = ..., hnsw_config: _Optional[_Union[_common_pb2.HnswConfigParams, _Mapping]] = ...) -> None: ...

class CreateCollectionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCollectionInfoRequest(_message.Message):
    __slots__ = ("collection_name",)
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    def __init__(self, collection_name: _Optional[str] = ...) -> None: ...

class GetCollectionInfoResponse(_message.Message):
    __slots__ = ("collection_name", "status", "vector_count", "segment_count", "disk_size_bytes", "ram_footprint_bytes", "config", "distance_metric")
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COUNT_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_COUNT_FIELD_NUMBER: _ClassVar[int]
    DISK_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    RAM_FOOTPRINT_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    status: CollectionStatus
    vector_count: int
    segment_count: int
    disk_size_bytes: int
    ram_footprint_bytes: int
    config: _common_pb2.HnswConfigParams
    distance_metric: _common_pb2.DistanceMetric
    def __init__(self, collection_name: _Optional[str] = ..., status: _Optional[_Union[CollectionStatus, str]] = ..., vector_count: _Optional[int] = ..., segment_count: _Optional[int] = ..., disk_size_bytes: _Optional[int] = ..., ram_footprint_bytes: _Optional[int] = ..., config: _Optional[_Union[_common_pb2.HnswConfigParams, _Mapping]] = ..., distance_metric: _Optional[_Union[_common_pb2.DistanceMetric, str]] = ...) -> None: ...

class ListCollectionsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListCollectionsResponse(_message.Message):
    __slots__ = ("collections",)
    COLLECTIONS_FIELD_NUMBER: _ClassVar[int]
    collections: _containers.RepeatedCompositeFieldContainer[CollectionDescription]
    def __init__(self, collections: _Optional[_Iterable[_Union[CollectionDescription, _Mapping]]] = ...) -> None: ...

class CollectionDescription(_message.Message):
    __slots__ = ("name", "vector_count", "status", "dimensions", "distance_metric")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_COUNT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    name: str
    vector_count: int
    status: CollectionStatus
    dimensions: int
    distance_metric: _common_pb2.DistanceMetric
    def __init__(self, name: _Optional[str] = ..., vector_count: _Optional[int] = ..., status: _Optional[_Union[CollectionStatus, str]] = ..., dimensions: _Optional[int] = ..., distance_metric: _Optional[_Union[_common_pb2.DistanceMetric, str]] = ...) -> None: ...

class DeleteCollectionRequest(_message.Message):
    __slots__ = ("collection_name",)
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    collection_name: str
    def __init__(self, collection_name: _Optional[str] = ...) -> None: ...

class DeleteCollectionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
