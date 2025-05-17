from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DistanceMetric(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISTANCE_METRIC_UNSPECIFIED: _ClassVar[DistanceMetric]
    COSINE: _ClassVar[DistanceMetric]
    EUCLIDEAN_L2: _ClassVar[DistanceMetric]

class StatusCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_CODE_UNSPECIFIED: _ClassVar[StatusCode]
    OK: _ClassVar[StatusCode]
    ERROR: _ClassVar[StatusCode]
    NOT_FOUND: _ClassVar[StatusCode]
    INVALID_ARGUMENT: _ClassVar[StatusCode]
DISTANCE_METRIC_UNSPECIFIED: DistanceMetric
COSINE: DistanceMetric
EUCLIDEAN_L2: DistanceMetric
STATUS_CODE_UNSPECIFIED: StatusCode
OK: StatusCode
ERROR: StatusCode
NOT_FOUND: StatusCode
INVALID_ARGUMENT: StatusCode

class Vector(_message.Message):
    __slots__ = ("elements",)
    ELEMENTS_FIELD_NUMBER: _ClassVar[int]
    elements: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, elements: _Optional[_Iterable[float]] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ("fields",)
    class FieldsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.MessageMap[str, _struct_pb2.Value]
    def __init__(self, fields: _Optional[_Mapping[str, _struct_pb2.Value]] = ...) -> None: ...

class PointStruct(_message.Message):
    __slots__ = ("id", "vector", "payload")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: Vector
    payload: Payload
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Union[Vector, _Mapping]] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class ScoredPoint(_message.Message):
    __slots__ = ("id", "vector", "payload", "score", "version")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: Vector
    payload: Payload
    score: float
    version: int
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Union[Vector, _Mapping]] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ..., score: _Optional[float] = ..., version: _Optional[int] = ...) -> None: ...

class Filter(_message.Message):
    __slots__ = ("must_match_exact",)
    class MustMatchExactEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    MUST_MATCH_EXACT_FIELD_NUMBER: _ClassVar[int]
    must_match_exact: _containers.MessageMap[str, _struct_pb2.Value]
    def __init__(self, must_match_exact: _Optional[_Mapping[str, _struct_pb2.Value]] = ...) -> None: ...

class HnswConfigParams(_message.Message):
    __slots__ = ("m", "ef_construction", "ef_search", "ml", "seed", "vector_dim", "m_max0")
    M_FIELD_NUMBER: _ClassVar[int]
    EF_CONSTRUCTION_FIELD_NUMBER: _ClassVar[int]
    EF_SEARCH_FIELD_NUMBER: _ClassVar[int]
    ML_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DIM_FIELD_NUMBER: _ClassVar[int]
    M_MAX0_FIELD_NUMBER: _ClassVar[int]
    m: int
    ef_construction: int
    ef_search: int
    ml: float
    seed: int
    vector_dim: int
    m_max0: int
    def __init__(self, m: _Optional[int] = ..., ef_construction: _Optional[int] = ..., ef_search: _Optional[int] = ..., ml: _Optional[float] = ..., seed: _Optional[int] = ..., vector_dim: _Optional[int] = ..., m_max0: _Optional[int] = ...) -> None: ...

class SearchParams(_message.Message):
    __slots__ = ("ef_search",)
    EF_SEARCH_FIELD_NUMBER: _ClassVar[int]
    ef_search: int
    def __init__(self, ef_search: _Optional[int] = ...) -> None: ...

class PointOperationStatus(_message.Message):
    __slots__ = ("point_id", "status_code", "error_message")
    POINT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    point_id: str
    status_code: StatusCode
    error_message: str
    def __init__(self, point_id: _Optional[str] = ..., status_code: _Optional[_Union[StatusCode, str]] = ..., error_message: _Optional[str] = ...) -> None: ...
