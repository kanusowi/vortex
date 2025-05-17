"""
Pydantic models for the Vortex SDK.

These models provide Pythonic, type-hinted representations of the
data structures used in the Vortex gRPC API. They are used for
both request and response validation and serialization.
"""
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from pydantic import BaseModel, Field # type: ignore

# --- Enums ---

class DistanceMetric(str, Enum):
    """Distance metric for comparing vectors."""
    COSINE = "COSINE"
    EUCLIDEAN_L2 = "EUCLIDEAN_L2"
    # Placeholder for unspecified, usually handled by server or default
    # DISTANCE_METRIC_UNSPECIFIED = "DISTANCE_METRIC_UNSPECIFIED"

class CollectionStatus(str, Enum):
    """Status of a collection."""
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"
    OPTIMIZING = "OPTIMIZING"
    CREATING = "CREATING"
    # Placeholder for unspecified
    # COLLECTION_STATUS_UNSPECIFIED = "COLLECTION_STATUS_UNSPECIFIED"

class StatusCode(str, Enum):
    """General status codes for operations."""
    OK = "OK"
    ERROR = "ERROR"
    NOT_FOUND = "NOT_FOUND"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    # Placeholder for unspecified
    # STATUS_CODE_UNSPECIFIED = "STATUS_CODE_UNSPECIFIED"

# --- Models from common.proto ---

class Vector(BaseModel):
    """Represents a dense vector."""
    elements: List[float] = Field(default_factory=list)

# google.protobuf.Value can be null, number, string, boolean, struct (object), or list.
PayloadValue = Union[None, float, str, bool, Dict[str, Any], List[Any]]

class Payload(BaseModel):
    """
    Represents a payload, a map of string keys to arbitrary JSON-like values.
    """
    fields: Dict[str, PayloadValue] = Field(default_factory=dict)

class PointStruct(BaseModel):
    """Represents a single point (vector with ID and optional payload)."""
    id: str
    vector: Vector
    payload: Optional[Payload] = None

class ScoredPoint(BaseModel):
    """Represents a point returned from a search query, including its score."""
    id: str
    vector: Optional[Vector] = None
    payload: Optional[Payload] = None
    score: float
    version: Optional[int] = None # Corresponds to uint64 in proto

class Filter(BaseModel):
    """
    Represents a filter for querying points.
    Initially simple, will be expanded for advanced filtering.
    """
    must_match_exact: Optional[Dict[str, PayloadValue]] = None
    # TODO: Add 'should_match_exact', 'must_not_match_exact', range filters, etc.

class HnswConfigParams(BaseModel):
    """HNSW configuration parameters."""
    m: int = Field(..., gt=0)  # Number of connections per node.
    ef_construction: int = Field(..., gt=0)  # Size of the dynamic list for HNSW construction.
    ef_search: int = Field(..., gt=0)  # Size of the dynamic list for HNSW search.
    ml: float = Field(..., gt=0)  # Normalization factor for level generation.
    seed: Optional[int] = None  # Seed for random number generation (optional). Corresponds to uint64
    vector_dim: int = Field(..., gt=0) # Dimensionality of the vectors.
    m_max0: int = Field(..., gt=0) # Max connections for layer 0.

class PointOperationStatus(BaseModel):
    """Status of an operation on a single point."""
    point_id: str
    status_code: StatusCode
    error_message: Optional[str] = None

# --- Models from collections_service.proto ---

class CollectionInfo(BaseModel):
    """Detailed information about a collection."""
    collection_name: str
    status: CollectionStatus
    vector_count: int # Corresponds to uint64
    segment_count: int # Corresponds to uint64
    disk_size_bytes: int # Corresponds to uint64
    ram_footprint_bytes: int # Corresponds to uint64
    config: HnswConfigParams
    distance_metric: DistanceMetric

class CollectionDescription(BaseModel):
    """Brief description of a collection for listing."""
    name: str
    vector_count: int # Corresponds to uint64
    status: CollectionStatus
    dimensions: int # Corresponds to uint32
    distance_metric: DistanceMetric

# --- Request/Response specific models (if not directly using the above) ---
# These might be defined later if complex request/response structures are needed
# that don't map directly to the core entities.

# Example: If CreateCollectionRequest was more complex than just its fields
# class CreateCollectionRequestModel(BaseModel):
#     collection_name: str
#     vector_dimensions: int
#     distance_metric: DistanceMetric
#     hnsw_config: Optional[HnswConfigParams] = None

# Placeholder for future models
class SearchParams(BaseModel):
    """Additional parameters for search operations."""
    ef_search: Optional[int] = Field(None, gt=0) # Corresponds to HNSW ef_search, must be > 0 if set

# It's good practice to re-export all models for easier access
__all__ = [
    "DistanceMetric",
    "CollectionStatus",
    "StatusCode",
    "Vector",
    "PayloadValue",
    "Payload",
    "PointStruct",
    "ScoredPoint",
    "Filter",
    "HnswConfigParams",
    "PointOperationStatus",
    "CollectionInfo",
    "CollectionDescription",
    "SearchParams",
]
