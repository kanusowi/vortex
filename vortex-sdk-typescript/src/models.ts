/**
 * TypeScript interfaces and enums for the Vortex SDK.
 * These correspond to the gRPC messages and Pydantic models.
 */

// --- Enums ---

/**
 * Defines the distance metrics supported for vector similarity search.
 */
export enum DistanceMetric {
  /** Cosine similarity. */
  COSINE = "COSINE",
  /** Euclidean L2 distance. */
  EUCLIDEAN_L2 = "EUCLIDEAN_L2",
}

/**
 * Represents the operational status of a collection.
 */
export enum CollectionStatus {
  /** Collection is healthy and queryable. */
  GREEN = "GREEN",
  /** Collection is partially available or degraded. */
  YELLOW = "YELLOW",
  /** Collection is unavailable or in an error state. */
  RED = "RED",
  /** Collection is undergoing optimization. */
  OPTIMIZING = "OPTIMIZING",
  /** Collection is currently being created. */
  CREATING = "CREATING",
}

/**
 * Represents the status code of an operation.
 */
export enum StatusCode {
  /** Operation completed successfully. */
  OK = "OK",
  /** Operation failed due to an error. */
  ERROR = "ERROR",
  /** Requested resource was not found. */
  NOT_FOUND = "NOT_FOUND",
  /** Invalid argument provided in the request. */
  INVALID_ARGUMENT = "INVALID_ARGUMENT",
}

// --- Models from common.proto ---

/**
 * Represents a dense vector.
 */
export interface Vector {
  /** The elements of the vector. */
  elements: number[];
}

/**
 * Represents a value within a payload.
 * Can be null, number, string, boolean, a nested object, or a list of such values.
 */
export type PayloadValue = null | number | string | boolean | { [key: string]: PayloadValue } | PayloadValue[];

/**
 * Represents the payload (metadata) associated with a point.
 */
export interface Payload {
  /** A map of field names to their corresponding payload values. */
  fields: { [key: string]: PayloadValue };
}

/**
 * Represents a point to be stored in a collection, including its ID, vector, and optional payload.
 */
export interface PointStruct {
  /** The unique identifier for the point. */
  id: string;
  /** The vector representation of the point. */
  vector: Vector;
  /** Optional payload associated with the point. */
  payload?: Payload | null;
}

/**
 * Represents a point retrieved from a search operation, including its score and version.
 */
export interface ScoredPoint {
  /** The unique identifier for the point. */
  id: string;
  /** Optional vector representation of the point. */
  vector?: Vector | null;
  /** Optional payload associated with the point. */
  payload?: Payload | null;
  /** The similarity score of the point with respect to the query. */
  score: number;
  /** 
   * The version of the point. 
   * Corresponds to uint64 in proto; handle potential large numbers if necessary in consuming code.
   */
  version?: number | null; 
}

/**
 * Represents filtering conditions for a search query.
 */
export interface Filter {
  /** 
   * Conditions where all key-value pairs must match exactly.
   * Example: `{ "genre": "sci-fi", "year": 2023 }`
   */
  mustMatchExact?: { [key: string]: PayloadValue } | null;
  // TODO: Add 'should_match_exact', 'must_not_match_exact', range filters, etc. as per strategicRoadmap.md
}

/**
 * Represents the HNSW (Hierarchical Navigable Small World) indexing configuration parameters.
 */
export interface HnswConfigParams {
  /** 
   * Number of bi-directional links created for every new element during construction.
   * Higher M can lead to better recall but higher memory usage and slower indexing.
   */
  m: number;
  /** 
   * Size of the dynamic list for the nearest neighbors (used during construction).
   * Higher efConstruction can lead to better index quality but slower indexing.
   */
  efConstruction: number; 
  /** 
   * Size of the dynamic list for the nearest neighbors (used during search).
   * Higher efSearch can lead to better recall but slower search.
   */
  efSearch: number;
  /** 
   * Normalization factor for level generation. 
   * Controls the probability distribution of assigning layers to new points.
   */
  ml: number;
  /** 
   * Optional seed for the random number generator used in HNSW construction.
   * Corresponds to uint64 in proto.
   */
  seed?: number | null; 
  /** The dimensionality of the vectors in the collection. */
  vectorDim: number;
  /** 
   * Number of bi-directional links for layer 0.
   * Typically 2 * m.
   */
  mMax0: number;
}

/**
 * Represents the status of an operation performed on a single point.
 */
export interface PointOperationStatus {
  /** The ID of the point involved in the operation. */
  pointId: string;
  /** The status code of the operation for this point. */
  statusCode: StatusCode;
  /** An optional error message if the operation failed for this point. */
  errorMessage?: string | null;
}

/**
 * Represents additional parameters for a search operation.
 */
export interface SearchParams {
  /** 
   * Optional HNSW ef_search parameter override for a specific query.
   * If provided, this value will be used for the HNSW search instead of the collection's default efSearch.
   */
  hnsw_ef?: number; 
  // 'exact' field is not currently in the common.proto SearchParams definition.
}

// --- Models from collections_service.proto ---

/**
 * Represents detailed information about a collection.
 */
export interface CollectionInfo {
  /** The name of the collection. */
  collectionName: string;
  /** The current operational status of the collection. */
  status: CollectionStatus;
  /** 
   * The total number of vectors in the collection. 
   * Corresponds to uint64 in proto.
   */
  vectorCount: number; 
  /** 
   * The number of segments in the collection. 
   * Corresponds to uint64 in proto.
   */
  segmentCount: number; 
  /** 
   * The total size of the collection on disk in bytes. 
   * Corresponds to uint64 in proto.
   */
  diskSizeBytes: number; 
  /** 
   * The estimated RAM footprint of the collection in bytes. 
   * Corresponds to uint64 in proto.
   */
  ramFootprintBytes: number; 
  /** The HNSW configuration of the collection. */
  config: HnswConfigParams;
  /** The distance metric used by the collection. */
  distanceMetric: DistanceMetric;
}

/**
 * Represents a brief description of a collection, typically used in listings.
 */
export interface CollectionDescription {
  /** The name of the collection. */
  name: string;
  /** 
   * The total number of vectors in the collection. 
   * Corresponds to uint64 in proto.
   */
  vectorCount: number; 
  /** The current operational status of the collection. */
  status: CollectionStatus;
  /** 
   * The dimensionality of the vectors in the collection. 
   * Corresponds to uint32 in proto.
   */
  dimensions: number; 
  /** The distance metric used by the collection. */
  distanceMetric: DistanceMetric;
}

// TODO: Add request/response specific models if needed
// TODO: Add models for PointsService specific messages as client methods are implemented
