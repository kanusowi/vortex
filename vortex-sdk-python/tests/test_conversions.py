"""
Unit tests for conversion functions in vortex_sdk.conversions.
"""
import pytest
from google.protobuf import struct_pb2

from vortex_sdk import models
from vortex_sdk import conversions
from vortex_sdk._grpc.vortex.api.v1 import common_pb2
from vortex_sdk._grpc.vortex.api.v1 import collections_service_pb2

# --- Test Data ---

def get_sample_pydantic_vector() -> models.Vector:
    return models.Vector(elements=[1.1, 2.2, 3.3])

def get_sample_grpc_vector() -> common_pb2.Vector:
    return common_pb2.Vector(elements=[1.1, 2.2, 3.3])

def get_sample_pydantic_payload() -> models.Payload:
    return models.Payload(fields={
        "string_field": "hello",
        "int_field": 123,
        "float_field": 45.67,
        "bool_field": True,
        "null_field": None,
        "list_field": ["a", 1, False, None],
        "struct_field": {"nested_str": "world", "nested_int": 789}
    })

def get_sample_grpc_payload() -> common_pb2.Payload:
    payload_pb = common_pb2.Payload()
    payload_pb.fields["string_field"].string_value = "hello"
    payload_pb.fields["int_field"].number_value = 123
    payload_pb.fields["float_field"].number_value = 45.67
    payload_pb.fields["bool_field"].bool_value = True
    payload_pb.fields["null_field"].null_value = struct_pb2.NULL_VALUE
    
    list_val = struct_pb2.Value()
    list_val.list_value.values.add().string_value = "a"
    list_val.list_value.values.add().number_value = 1
    list_val.list_value.values.add().bool_value = False
    list_val.list_value.values.add().null_value = struct_pb2.NULL_VALUE
    payload_pb.fields["list_field"].CopyFrom(list_val)

    struct_val = struct_pb2.Value()
    struct_val.struct_value.fields["nested_str"].string_value = "world"
    struct_val.struct_value.fields["nested_int"].number_value = 789
    payload_pb.fields["struct_field"].CopyFrom(struct_val)
    return payload_pb

def get_sample_pydantic_point_struct() -> models.PointStruct:
    return models.PointStruct(
        id="p1",
        vector=get_sample_pydantic_vector(),
        payload=get_sample_pydantic_payload()
    )

def get_sample_grpc_point_struct() -> common_pb2.PointStruct:
    point_pb = common_pb2.PointStruct(
        id="p1",
        vector=get_sample_grpc_vector()
    )
    point_pb.payload.CopyFrom(get_sample_grpc_payload())
    return point_pb

def get_sample_pydantic_hnsw_config() -> models.HnswConfigParams:
    return models.HnswConfigParams(
        m=16, ef_construction=200, ef_search=100, ml=0.5, vector_dim=3, m_max0=32, seed=42
    )

def get_sample_grpc_hnsw_config() -> common_pb2.HnswConfigParams:
    return common_pb2.HnswConfigParams(
        m=16, ef_construction=200, ef_search=100, ml=0.5, seed=42, vector_dim=3, m_max0=32
    )

# --- Test Cases ---

def test_vector_conversion():
    pydantic_vec = get_sample_pydantic_vector()
    grpc_vec = conversions.pydantic_to_grpc_vector(pydantic_vec)
    assert list(grpc_vec.elements) == pytest.approx(pydantic_vec.elements)

    pydantic_vec_converted_back = conversions.grpc_to_pydantic_vector(grpc_vec)
    assert pydantic_vec_converted_back.elements == pytest.approx(pydantic_vec.elements)

def test_payload_conversion():
    pydantic_payload = get_sample_pydantic_payload()
    grpc_payload = conversions.pydantic_to_grpc_payload(pydantic_payload)
    
    # Check a few fields
    assert grpc_payload.fields["string_field"].string_value == "hello"
    assert grpc_payload.fields["int_field"].number_value == 123
    assert grpc_payload.fields["bool_field"].bool_value is True
    assert grpc_payload.fields["null_field"].HasField("null_value")
    assert len(grpc_payload.fields["list_field"].list_value.values) == 4
    assert grpc_payload.fields["struct_field"].struct_value.fields["nested_str"].string_value == "world"

    pydantic_payload_converted_back = conversions.grpc_to_pydantic_payload(grpc_payload)
    assert pydantic_payload_converted_back == pydantic_payload

def test_point_struct_conversion():
    pydantic_point = get_sample_pydantic_point_struct()
    grpc_point = conversions.pydantic_to_grpc_point_struct(pydantic_point)
    assert grpc_point.id == pydantic_point.id
    assert list(grpc_point.vector.elements) == pytest.approx(pydantic_point.vector.elements)
    assert grpc_point.payload.fields["string_field"].string_value == "hello"

    pydantic_point_converted_back = conversions.grpc_to_pydantic_point_struct(grpc_point)
    # Compare fields individually due to potential float precision in payload
    assert pydantic_point_converted_back.id == pydantic_point.id
    assert pydantic_point_converted_back.vector.elements == pytest.approx(pydantic_point.vector.elements)
    assert pydantic_point_converted_back.payload.fields["string_field"] == pydantic_point.payload.fields["string_field"]
    assert pydantic_point_converted_back.payload.fields["int_field"] == pydantic_point.payload.fields["int_field"]
    assert pydantic_point_converted_back.payload.fields["float_field"] == pytest.approx(pydantic_point.payload.fields["float_field"])
    # ... compare other payload fields as needed or use a helper for deep dict comparison with approx

    # Test with no payload
    pydantic_point_no_payload = models.PointStruct(id="p2", vector=get_sample_pydantic_vector())
    grpc_point_no_payload = conversions.pydantic_to_grpc_point_struct(pydantic_point_no_payload)
    assert not grpc_point_no_payload.HasField("payload")
    pydantic_point_np_back = conversions.grpc_to_pydantic_point_struct(grpc_point_no_payload)
    assert pydantic_point_np_back.payload is None


def test_scored_point_conversion():
    grpc_sp = common_pb2.ScoredPoint(
        id="sp1",
        vector=get_sample_grpc_vector(),
        score=0.88,
        version=10
    )
    grpc_sp.payload.CopyFrom(get_sample_grpc_payload())
    
    pydantic_sp = conversions.grpc_to_pydantic_scored_point(grpc_sp)
    assert pydantic_sp.id == "sp1"
    assert pydantic_sp.score == pytest.approx(0.88)
    assert pydantic_sp.version == 10
    assert pydantic_sp.vector.elements == pytest.approx([1.1, 2.2, 3.3])
    assert pydantic_sp.payload.fields["string_field"] == "hello"

    # Test minimal ScoredPoint (no vector, payload, version)
    grpc_sp_minimal = common_pb2.ScoredPoint(id="sp2", score=0.77)
    pydantic_sp_minimal = conversions.grpc_to_pydantic_scored_point(grpc_sp_minimal)
    assert pydantic_sp_minimal.id == "sp2"
    assert pydantic_sp_minimal.score == pytest.approx(0.77)
    assert pydantic_sp_minimal.vector is None
    assert pydantic_sp_minimal.payload is None
    assert pydantic_sp_minimal.version is None


def test_hnsw_config_conversion():
    pydantic_config = get_sample_pydantic_hnsw_config()
    grpc_config = conversions.pydantic_to_grpc_hnsw_config(pydantic_config)
    assert grpc_config.m == pydantic_config.m
    assert grpc_config.ef_construction == pydantic_config.ef_construction
    assert grpc_config.seed == pydantic_config.seed

    pydantic_config_converted_back = conversions.grpc_to_pydantic_hnsw_config(grpc_config)
    assert pydantic_config_converted_back == pydantic_config

    # Test without seed
    pydantic_config_no_seed = models.HnswConfigParams(
        m=8, ef_construction=100, ef_search=50, ml=0.3, vector_dim=2, m_max0=16
    )
    grpc_config_no_seed = conversions.pydantic_to_grpc_hnsw_config(pydantic_config_no_seed)
    assert not grpc_config_no_seed.HasField("seed")
    pydantic_config_ns_back = conversions.grpc_to_pydantic_hnsw_config(grpc_config_no_seed)
    assert pydantic_config_ns_back.seed is None


def test_distance_metric_conversion():
    assert conversions.pydantic_to_grpc_distance_metric(models.DistanceMetric.COSINE) == common_pb2.DistanceMetric.COSINE
    assert conversions.pydantic_to_grpc_distance_metric(models.DistanceMetric.EUCLIDEAN_L2) == common_pb2.DistanceMetric.EUCLIDEAN_L2

    assert conversions.grpc_to_pydantic_distance_metric(common_pb2.DistanceMetric.COSINE) == models.DistanceMetric.COSINE
    assert conversions.grpc_to_pydantic_distance_metric(common_pb2.DistanceMetric.EUCLIDEAN_L2) == models.DistanceMetric.EUCLIDEAN_L2
    # Test default for unspecified
    assert conversions.grpc_to_pydantic_distance_metric(common_pb2.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED) == models.DistanceMetric.COSINE

def test_collection_status_conversion():
    assert conversions.grpc_to_pydantic_collection_status(collections_service_pb2.CollectionStatus.GREEN) == models.CollectionStatus.GREEN
    assert conversions.grpc_to_pydantic_collection_status(collections_service_pb2.CollectionStatus.OPTIMIZING) == models.CollectionStatus.OPTIMIZING
    # Test default for unspecified
    assert conversions.grpc_to_pydantic_collection_status(collections_service_pb2.CollectionStatus.COLLECTION_STATUS_UNSPECIFIED) == models.CollectionStatus.GREEN

def test_collection_info_conversion():
    grpc_info = collections_service_pb2.GetCollectionInfoResponse(
        collection_name="coll1",
        status=collections_service_pb2.CollectionStatus.YELLOW,
        vector_count=1000,
        segment_count=2,
        disk_size_bytes=2048,
        ram_footprint_bytes=1024,
        config=get_sample_grpc_hnsw_config(),
        distance_metric=common_pb2.DistanceMetric.EUCLIDEAN_L2
    )
    pydantic_info = conversions.grpc_to_pydantic_collection_info(grpc_info)
    assert pydantic_info.collection_name == "coll1"
    assert pydantic_info.status == models.CollectionStatus.YELLOW
    assert pydantic_info.vector_count == 1000
    assert pydantic_info.config.m == 16
    assert pydantic_info.distance_metric == models.DistanceMetric.EUCLIDEAN_L2

def test_collection_description_conversion():
    grpc_desc = collections_service_pb2.CollectionDescription(
        name="desc_coll",
        vector_count=500,
        status=collections_service_pb2.CollectionStatus.CREATING,
        dimensions=128,
        distance_metric=common_pb2.DistanceMetric.COSINE
    )
    pydantic_desc = conversions.grpc_to_pydantic_collection_description(grpc_desc)
    assert pydantic_desc.name == "desc_coll"
    assert pydantic_desc.vector_count == 500
    assert pydantic_desc.status == models.CollectionStatus.CREATING
    assert pydantic_desc.dimensions == 128
    assert pydantic_desc.distance_metric == models.DistanceMetric.COSINE

def test_filter_conversion():
    pydantic_filter = models.Filter(must_match_exact={"key1": "value1", "key2": 123})
    grpc_filter = conversions.pydantic_to_grpc_filter(pydantic_filter)
    assert grpc_filter is not None
    assert grpc_filter.must_match_exact["key1"].string_value == "value1"
    assert grpc_filter.must_match_exact["key2"].number_value == 123

    pydantic_filter_back = conversions.grpc_to_pydantic_filter(grpc_filter)
    assert pydantic_filter_back.must_match_exact == pydantic_filter.must_match_exact

    # Test None filter
    assert conversions.pydantic_to_grpc_filter(None) is None
    assert conversions.pydantic_to_grpc_filter(models.Filter()) is None
    
    # Test empty must_match_exact (should still create a filter message if that's API expectation)
    # Current pydantic_to_grpc_filter returns None if must_match_exact is None or empty.
    # If an empty Filter message is needed, the logic would change.
    # For now, this behavior is fine.
    pydantic_filter_empty_dict = models.Filter(must_match_exact={})
    # If must_match_exact is an empty dict, it should still produce a Filter message
    # with an empty map, not None. Let's adjust the test or the function.
    # For now, let's assume the function should return a Filter with an empty map.
    # If the intention is to return None for empty filters, the function needs change.
    # Based on current function: it returns None if filter_model.must_match_exact is None.
    # If filter_model.must_match_exact is {}, it will create a Filter with an empty map.
    # The test was expecting None, but the code produces an empty Filter.
    # Let's adjust the test to reflect the code's behavior or clarify the desired behavior.
    # The current code for pydantic_to_grpc_filter:
    # if filter_model is None or filter_model.must_match_exact is None: return None
    # ... if filter_model.must_match_exact: # This will be true for {}
    # So, if must_match_exact is {}, it will proceed.
    # The issue is that the loop `for k, v_pydantic in filter_model.must_match_exact.items():`
    # will not run for an empty dict, so `grpc_filter.must_match_exact` will be empty.
    # This is correct. The test was asserting `is None` which is wrong if an empty dict is passed.
    # If the dict is empty, it should return an empty filter, not None.
    # The original code returns None if filter_model.must_match_exact is None, not if it's empty.
    # Let's make the function return None if the must_match_exact dict is empty.
    # This seems more logical: no conditions means no filter.
    # I will modify the `pydantic_to_grpc_filter` function in the next step.
    # For now, I'll keep the test as is, expecting it to fail or be adjusted with the function.
    # The current test `assert conversions.pydantic_to_grpc_filter(pydantic_filter_empty_dict) is None`
    # will fail if the function returns an empty Filter message.
    # Let's assume for now the function should return None for an empty dict.
    # This means the function `pydantic_to_grpc_filter` needs a slight change.
    # The current test is correct if the desired behavior is that an empty filter dict results in no filter.
    # The function pydantic_to_grpc_filter was updated to return None for empty must_match_exact.
    assert conversions.pydantic_to_grpc_filter(pydantic_filter_empty_dict) is None


def test_status_code_conversion():
    assert conversions.grpc_to_pydantic_status_code(common_pb2.StatusCode.OK) == models.StatusCode.OK
    assert conversions.grpc_to_pydantic_status_code(common_pb2.StatusCode.NOT_FOUND) == models.StatusCode.NOT_FOUND
    assert conversions.grpc_to_pydantic_status_code(common_pb2.StatusCode.STATUS_CODE_UNSPECIFIED) == models.StatusCode.ERROR # Default

def test_point_operation_status_conversion():
    grpc_pos = common_pb2.PointOperationStatus(
        point_id="p1_op",
        status_code=common_pb2.StatusCode.INVALID_ARGUMENT,
        error_message="Invalid vector dimensions"
    )
    pydantic_pos = conversions.grpc_to_pydantic_point_operation_status(grpc_pos)
    assert pydantic_pos.point_id == "p1_op"
    assert pydantic_pos.status_code == models.StatusCode.INVALID_ARGUMENT
    assert pydantic_pos.error_message == "Invalid vector dimensions"

    grpc_pos_ok = common_pb2.PointOperationStatus(
        point_id="p2_op",
        status_code=common_pb2.StatusCode.OK
    )
    pydantic_pos_ok = conversions.grpc_to_pydantic_point_operation_status(grpc_pos_ok)
    assert pydantic_pos_ok.error_message is None

def test_search_params_conversion():
    """Test conversion of SearchParams Pydantic model to gRPC."""
    # Test with ef_search set
    pydantic_sp = models.SearchParams(ef_search=120)
    grpc_sp = conversions.pydantic_to_grpc_search_params(pydantic_sp)
    assert grpc_sp is not None
    assert grpc_sp.HasField("ef_search")
    assert grpc_sp.ef_search == 120

    # Test with ef_search not set (should return None as per current logic)
    pydantic_sp_none = models.SearchParams()
    grpc_sp_none = conversions.pydantic_to_grpc_search_params(pydantic_sp_none)
    assert grpc_sp_none is None

    # Test with pydantic_sp itself being None
    grpc_sp_from_none_pydantic = conversions.pydantic_to_grpc_search_params(None)
    assert grpc_sp_from_none_pydantic is None
