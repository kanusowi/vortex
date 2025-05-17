"""
Unit tests for Pydantic models in vortex_sdk.models.
"""
import pytest
from pydantic import ValidationError

from vortex_sdk import models

def test_vector_creation():
    """Test basic Vector model creation."""
    v_data = {"elements": [1.0, 2.0, 3.0]}
    v = models.Vector(**v_data)
    assert v.elements == [1.0, 2.0, 3.0]

    v_empty = models.Vector()
    assert v_empty.elements == []

def test_payload_creation():
    """Test Payload model creation with various value types."""
    p_data = {
        "fields": {
            "name": "test_item",
            "count": 10,
            "price": 19.99,
            "is_active": True,
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value", "nested_key": {"sub_key": 100}},
            "nullable_field": None,
        }
    }
    p = models.Payload(**p_data)
    assert p.fields["name"] == "test_item"
    assert p.fields["count"] == 10
    assert p.fields["price"] == 19.99
    assert p.fields["is_active"] is True
    assert p.fields["tags"] == ["tag1", "tag2"]
    assert p.fields["metadata"] == {"key": "value", "nested_key": {"sub_key": 100}}
    assert p.fields["nullable_field"] is None

    p_empty = models.Payload()
    assert p_empty.fields == {}

def test_point_struct_creation():
    """Test PointStruct model creation."""
    ps_data = {
        "id": "point1",
        "vector": {"elements": [0.1, 0.2]},
        "payload": {"fields": {"category": "A"}}
    }
    ps = models.PointStruct(**ps_data)
    assert ps.id == "point1"
    assert ps.vector.elements == [0.1, 0.2]
    assert ps.payload.fields["category"] == "A"

    ps_no_payload_data = {
        "id": "point2",
        "vector": {"elements": [0.3, 0.4]},
    }
    ps_no_payload = models.PointStruct(**ps_no_payload_data)
    assert ps_no_payload.id == "point2"
    assert ps_no_payload.payload is None

    with pytest.raises(ValidationError):
        models.PointStruct(id="point3") # Missing vector

    with pytest.raises(ValidationError):
        models.PointStruct(vector={"elements": [1.0]}) # Missing id

def test_scored_point_creation():
    """Test ScoredPoint model creation."""
    sp_data = {
        "id": "point_s1",
        "vector": {"elements": [0.5, 0.6]},
        "payload": {"fields": {"source": "search"}},
        "score": 0.95,
        "version": 123
    }
    sp = models.ScoredPoint(**sp_data)
    assert sp.id == "point_s1"
    assert sp.score == 0.95
    assert sp.version == 123
    assert sp.vector.elements == [0.5, 0.6]
    assert sp.payload.fields["source"] == "search"

    sp_minimal_data = {
        "id": "point_s2",
        "score": 0.88
    }
    sp_minimal = models.ScoredPoint(**sp_minimal_data)
    assert sp_minimal.vector is None
    assert sp_minimal.payload is None
    assert sp_minimal.version is None

def test_hnsw_config_params_creation():
    """Test HnswConfigParams model creation and validation."""
    config_data = {
        "m": 16,
        "ef_construction": 200,
        "ef_search": 100,
        "ml": 0.5,
        "vector_dim": 128,
        "m_max0": 32
    }
    config = models.HnswConfigParams(**config_data)
    assert config.m == 16
    assert config.seed is None

    config_with_seed = models.HnswConfigParams(**config_data, seed=42)
    assert config_with_seed.seed == 42

    with pytest.raises(ValidationError): # m must be > 0
        models.HnswConfigParams(**{**config_data, "m": 0})
    
    with pytest.raises(ValidationError): # vector_dim must be > 0
        models.HnswConfigParams(**{**config_data, "vector_dim": 0})

def test_collection_info_creation():
    """Test CollectionInfo model creation."""
    ci_data = {
        "collection_name": "test_coll",
        "status": models.CollectionStatus.GREEN,
        "vector_count": 1000,
        "segment_count": 2,
        "disk_size_bytes": 102400,
        "ram_footprint_bytes": 51200,
        "config": {
            "m": 16, "ef_construction": 200, "ef_search": 100,
            "ml": 0.5, "vector_dim": 768, "m_max0": 32
        },
        "distance_metric": models.DistanceMetric.COSINE
    }
    ci = models.CollectionInfo(**ci_data)
    assert ci.collection_name == "test_coll"
    assert ci.status == models.CollectionStatus.GREEN
    assert ci.config.vector_dim == 768

def test_filter_creation():
    """Test Filter model creation."""
    f_data = {
        "must_match_exact": {
            "color": "blue",
            "count": {"gt": 5} # Example of a more complex value, though current Filter is exact
        }
    }
    # Current Filter model only supports exact matches with PayloadValue
    # So the "count" field above would be treated as a struct if not a simple value
    f_simple_data = {
         "must_match_exact": {
            "color": "blue",
            "processed": True
        }
    }
    f = models.Filter(**f_simple_data)
    assert f.must_match_exact["color"] == "blue"
    assert f.must_match_exact["processed"] is True

    f_empty = models.Filter()
    assert f_empty.must_match_exact is None

    f_none = models.Filter(must_match_exact=None)
    assert f_none.must_match_exact is None

def test_search_params_creation():
    """Test SearchParams model creation and validation."""
    # Test empty creation
    sp_empty = models.SearchParams()
    assert sp_empty.ef_search is None

    # Test with ef_search
    sp_with_ef = models.SearchParams(ef_search=100)
    assert sp_with_ef.ef_search == 100

    # Test validation (ef_search > 0)
    with pytest.raises(ValidationError):
        models.SearchParams(ef_search=0)
    
    with pytest.raises(ValidationError):
        models.SearchParams(ef_search=-10)

    # Test valid creation with positive ef_search
    sp_valid = models.SearchParams(ef_search=1)
    assert sp_valid.ef_search == 1


def test_enums():
    """Test enum string representations."""
    assert models.DistanceMetric.COSINE.value == "COSINE"
    assert models.CollectionStatus.OPTIMIZING.value == "OPTIMIZING"
    assert models.StatusCode.NOT_FOUND.value == "NOT_FOUND"
