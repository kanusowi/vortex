"""
Vortex Python SDK
"""
__version__ = "0.1.0"

from .client import VortexClient, AsyncVortexClient
from .exceptions import (
    VortexException,
    VortexConnectionError,
    VortexTimeoutError,
    VortexApiError,
    VortexClientConfigurationError,
)
from .models import (
    DistanceMetric,
    CollectionStatus,
    StatusCode,
    Vector,
    PayloadValue,
    Payload,
    PointStruct,
    ScoredPoint,
    Filter,
    HnswConfigParams,
    PointOperationStatus,
    CollectionInfo,
    CollectionDescription,
)

__all__ = [
    "VortexClient",
    "AsyncVortexClient",
    # Exceptions
    "VortexException",
    "VortexConnectionError",
    "VortexTimeoutError",
    "VortexApiError",
    "VortexClientConfigurationError",
    # Models & Enums
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
]
