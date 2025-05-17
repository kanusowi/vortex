"""
Conversion utilities between Pydantic models and gRPC messages.
"""
from typing import Dict, Any, List, Optional
from google.protobuf import struct_pb2

from vortex_sdk import models
from vortex_sdk._grpc.vortex.api.v1 import common_pb2
from vortex_sdk._grpc.vortex.api.v1 import collections_service_pb2
# from vortex_sdk._grpc.vortex.api.v1 import points_service_pb2 # For future use

# --- Enum Mappings ---

# DistanceMetric
_PYDANTIC_TO_GRPC_DISTANCE_METRIC_MAP: Dict[models.DistanceMetric, common_pb2.DistanceMetric.ValueType] = {
    models.DistanceMetric.COSINE: common_pb2.DistanceMetric.COSINE,
    models.DistanceMetric.EUCLIDEAN_L2: common_pb2.DistanceMetric.EUCLIDEAN_L2,
}
_GRPC_TO_PYDANTIC_DISTANCE_METRIC_MAP: Dict[common_pb2.DistanceMetric.ValueType, models.DistanceMetric] = {
    v: k for k, v in _PYDANTIC_TO_GRPC_DISTANCE_METRIC_MAP.items()
}
_GRPC_TO_PYDANTIC_DISTANCE_METRIC_MAP[common_pb2.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED] = models.DistanceMetric.COSINE # Default

# CollectionStatus
_GRPC_TO_PYDANTIC_COLLECTION_STATUS_MAP: Dict[collections_service_pb2.CollectionStatus.ValueType, models.CollectionStatus] = {
    collections_service_pb2.CollectionStatus.GREEN: models.CollectionStatus.GREEN,
    collections_service_pb2.CollectionStatus.YELLOW: models.CollectionStatus.YELLOW,
    collections_service_pb2.CollectionStatus.RED: models.CollectionStatus.RED,
    collections_service_pb2.CollectionStatus.OPTIMIZING: models.CollectionStatus.OPTIMIZING,
    collections_service_pb2.CollectionStatus.CREATING: models.CollectionStatus.CREATING,
    collections_service_pb2.CollectionStatus.COLLECTION_STATUS_UNSPECIFIED: models.CollectionStatus.GREEN, # Default
}

# StatusCode
_GRPC_TO_PYDANTIC_STATUS_CODE_MAP: Dict[common_pb2.StatusCode.ValueType, models.StatusCode] = {
    common_pb2.StatusCode.OK: models.StatusCode.OK,
    common_pb2.StatusCode.ERROR: models.StatusCode.ERROR,
    common_pb2.StatusCode.NOT_FOUND: models.StatusCode.NOT_FOUND,
    common_pb2.StatusCode.INVALID_ARGUMENT: models.StatusCode.INVALID_ARGUMENT,
    common_pb2.StatusCode.STATUS_CODE_UNSPECIFIED: models.StatusCode.ERROR, # Default
}

# --- Helper for google.protobuf.Value ---

def _grpc_value_to_pydantic(grpc_val: struct_pb2.Value) -> models.PayloadValue:
    if grpc_val.HasField("null_value"):
        return None
    if grpc_val.HasField("number_value"):
        return grpc_val.number_value
    if grpc_val.HasField("string_value"):
        return grpc_val.string_value
    if grpc_val.HasField("bool_value"):
        return grpc_val.bool_value
    if grpc_val.HasField("struct_value"):
        return {k: _grpc_value_to_pydantic(v) for k, v in grpc_val.struct_value.fields.items()}
    if grpc_val.HasField("list_value"):
        return [_grpc_value_to_pydantic(v) for v in grpc_val.list_value.values]
    return None # Should not happen for a valid Value

def _pydantic_value_to_grpc(pydantic_val: models.PayloadValue) -> struct_pb2.Value:
    grpc_val = struct_pb2.Value()
    if pydantic_val is None:
        grpc_val.null_value = struct_pb2.NULL_VALUE
    elif isinstance(pydantic_val, bool):
        grpc_val.bool_value = pydantic_val
    elif isinstance(pydantic_val, (int, float)):
        grpc_val.number_value = float(pydantic_val)
    elif isinstance(pydantic_val, str):
        grpc_val.string_value = pydantic_val
    elif isinstance(pydantic_val, list):
        for item in pydantic_val:
            grpc_val.list_value.values.add().CopyFrom(_pydantic_value_to_grpc(item))
    elif isinstance(pydantic_val, dict):
        for k, v in pydantic_val.items():
            grpc_val.struct_value.fields[k].CopyFrom(_pydantic_value_to_grpc(v))
    else:
        # This case should ideally not be reached if PayloadValue is used correctly
        grpc_val.string_value = str(pydantic_val)
    return grpc_val

# --- Conversion Functions ---

def pydantic_to_grpc_vector(vector: models.Vector) -> common_pb2.Vector:
    return common_pb2.Vector(elements=vector.elements)

def grpc_to_pydantic_vector(vector_pb: common_pb2.Vector) -> models.Vector:
    return models.Vector(elements=list(vector_pb.elements))

def pydantic_to_grpc_payload(payload: models.Payload) -> common_pb2.Payload:
    return common_pb2.Payload(
        fields={k: _pydantic_value_to_grpc(v) for k, v in payload.fields.items()}
    )

def grpc_to_pydantic_payload(payload_pb: common_pb2.Payload) -> models.Payload:
    return models.Payload(
        fields={k: _grpc_value_to_pydantic(v) for k, v in payload_pb.fields.items()}
    )

def pydantic_to_grpc_point_struct(point: models.PointStruct) -> common_pb2.PointStruct:
    point_pb = common_pb2.PointStruct(
        id=point.id,
        vector=pydantic_to_grpc_vector(point.vector)
    )
    if point.payload is not None:
        point_pb.payload.CopyFrom(pydantic_to_grpc_payload(point.payload))
    return point_pb

def grpc_to_pydantic_point_struct(point_pb: common_pb2.PointStruct) -> models.PointStruct:
    return models.PointStruct(
        id=point_pb.id,
        vector=grpc_to_pydantic_vector(point_pb.vector),
        payload=grpc_to_pydantic_payload(point_pb.payload) if point_pb.HasField("payload") else None
    )

def grpc_to_pydantic_scored_point(scored_point_pb: common_pb2.ScoredPoint) -> models.ScoredPoint:
    return models.ScoredPoint(
        id=scored_point_pb.id,
        vector=grpc_to_pydantic_vector(scored_point_pb.vector) if scored_point_pb.HasField("vector") else None,
        payload=grpc_to_pydantic_payload(scored_point_pb.payload) if scored_point_pb.HasField("payload") else None,
        score=scored_point_pb.score,
        version=scored_point_pb.version if scored_point_pb.HasField("version") else None
    )

def pydantic_to_grpc_hnsw_config(config: models.HnswConfigParams) -> common_pb2.HnswConfigParams:
    hnsw_config_pb = common_pb2.HnswConfigParams(
        m=config.m,
        ef_construction=config.ef_construction,
        ef_search=config.ef_search,
        ml=config.ml,
        vector_dim=config.vector_dim,
        m_max0=config.m_max0
    )
    if config.seed is not None:
        hnsw_config_pb.seed = config.seed
    return hnsw_config_pb

def grpc_to_pydantic_hnsw_config(config_pb: common_pb2.HnswConfigParams) -> models.HnswConfigParams:
    return models.HnswConfigParams(
        m=config_pb.m,
        ef_construction=config_pb.ef_construction,
        ef_search=config_pb.ef_search,
        ml=config_pb.ml,
        seed=config_pb.seed if config_pb.HasField("seed") else None,
        vector_dim=config_pb.vector_dim,
        m_max0=config_pb.m_max0
    )

def pydantic_to_grpc_distance_metric(metric: models.DistanceMetric) -> common_pb2.DistanceMetric.ValueType:
    return _PYDANTIC_TO_GRPC_DISTANCE_METRIC_MAP.get(metric, common_pb2.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED)

def grpc_to_pydantic_distance_metric(metric_pb: common_pb2.DistanceMetric.ValueType) -> models.DistanceMetric:
    return _GRPC_TO_PYDANTIC_DISTANCE_METRIC_MAP.get(metric_pb, models.DistanceMetric.COSINE) # Default to COSINE

def grpc_to_pydantic_collection_status(status_pb: collections_service_pb2.CollectionStatus.ValueType) -> models.CollectionStatus:
    return _GRPC_TO_PYDANTIC_COLLECTION_STATUS_MAP.get(status_pb, models.CollectionStatus.GREEN) # Default

def grpc_to_pydantic_collection_info(info_pb: collections_service_pb2.GetCollectionInfoResponse) -> models.CollectionInfo:
    return models.CollectionInfo(
        collection_name=info_pb.collection_name,
        status=grpc_to_pydantic_collection_status(info_pb.status),
        vector_count=info_pb.vector_count,
        segment_count=info_pb.segment_count,
        disk_size_bytes=info_pb.disk_size_bytes,
        ram_footprint_bytes=info_pb.ram_footprint_bytes,
        config=grpc_to_pydantic_hnsw_config(info_pb.config),
        distance_metric=grpc_to_pydantic_distance_metric(info_pb.distance_metric)
    )

def grpc_to_pydantic_collection_description(desc_pb: collections_service_pb2.CollectionDescription) -> models.CollectionDescription:
    return models.CollectionDescription(
        name=desc_pb.name,
        vector_count=desc_pb.vector_count,
        status=grpc_to_pydantic_collection_status(desc_pb.status),
        dimensions=desc_pb.dimensions,
        distance_metric=grpc_to_pydantic_distance_metric(desc_pb.distance_metric)
    )

def pydantic_to_grpc_filter(filter_model: Optional[models.Filter]) -> Optional[common_pb2.Filter]:
    if filter_model is None or \
       filter_model.must_match_exact is None or \
       not filter_model.must_match_exact: # Also return None if the dict is empty
        return None
    
    grpc_filter = common_pb2.Filter()
    # No need to check if filter_model.must_match_exact is True here,
    # as the check above ensures it's not None and not empty.
    for k, v_pydantic in filter_model.must_match_exact.items():
        grpc_filter.must_match_exact[k].CopyFrom(_pydantic_value_to_grpc(v_pydantic))
    return grpc_filter

def grpc_to_pydantic_filter(filter_pb: common_pb2.Filter) -> models.Filter:
    must_match_exact_pydantic: Optional[Dict[str, models.PayloadValue]] = None
    if filter_pb.must_match_exact:
        must_match_exact_pydantic = {
            k: _grpc_value_to_pydantic(v_grpc)
            for k, v_grpc in filter_pb.must_match_exact.items()
        }
    return models.Filter(must_match_exact=must_match_exact_pydantic)


def grpc_to_pydantic_status_code(status_code_pb: common_pb2.StatusCode.ValueType) -> models.StatusCode:
    return _GRPC_TO_PYDANTIC_STATUS_CODE_MAP.get(status_code_pb, models.StatusCode.ERROR) # Default

def grpc_to_pydantic_point_operation_status(status_pb: common_pb2.PointOperationStatus) -> models.PointOperationStatus:
    return models.PointOperationStatus(
        point_id=status_pb.point_id,
        status_code=grpc_to_pydantic_status_code(status_pb.status_code),
        error_message=status_pb.error_message if status_pb.HasField("error_message") else None
    )

def pydantic_to_grpc_search_params(params: Optional[models.SearchParams]) -> Optional[common_pb2.SearchParams]:
    if params is None:
        return None
    
    grpc_params = common_pb2.SearchParams()
    if params.ef_search is not None:
        grpc_params.ef_search = params.ef_search
    
    # Only return the object if it has at least one field set,
    # otherwise, an empty SearchParams might be sent, which could be
    # misinterpreted by the server or is simply unnecessary.
    # The proto defines ef_search as optional uint32.
    # If no fields are set, it's equivalent to not sending SearchParams.
    if not grpc_params.HasField("ef_search"): # Check if any optional field was actually set
        return None
        
    return grpc_params

# TODO: Add conversions for PointsService specific messages as they are implemented.
