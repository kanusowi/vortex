/**
 * @fileoverview Conversion utilities between TypeScript models defined in `src/models.ts`
 * and the gRPC JavaScript objects generated from `.proto` files.
 * This module ensures that data can be seamlessly translated between the SDK's
 * user-facing TypeScript interfaces and the underlying gRPC transport layer.
 */

import * as models from './models';
// Import generated gRPC types (adjust paths as necessary after generation)
import * as common_pb from './_grpc/vortex/api/v1/common_pb';
import * as collections_service_pb from './_grpc/vortex/api/v1/collections_service_pb';
// import * as points_service_pb from './_grpc/vortex/api/v1/points_service_pb'; // Commented out as SearchParams is from common_pb
import { Value } from 'google-protobuf/google/protobuf/struct_pb';
import * as struct_pb from 'google-protobuf/google/protobuf/struct_pb'; // Import the full module

// --- Enum Mappings ---
// The generated _pb.js files export enums as objects (e.g., DistanceMetricMap)
// where keys are string names and values are numbers.
// The .d.ts files might declare these as enums or as const objects.
// Assuming common_pb.DistanceMetric is the enum-like object from generated code.

/**
 * Maps TypeScript `DistanceMetric` enum to gRPC `DistanceMetric` enum.
 * @type {Record<models.DistanceMetric, common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap]>}
 * @internal
 */
const tsToGrpcDistanceMetricMap: Record<models.DistanceMetric, common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap]> = {
  [models.DistanceMetric.COSINE]: common_pb.DistanceMetric.COSINE,
  [models.DistanceMetric.EUCLIDEAN_L2]: common_pb.DistanceMetric.EUCLIDEAN_L2,
};

/**
 * Maps gRPC `DistanceMetric` enum to TypeScript `DistanceMetric` enum.
 * Defaults `DISTANCE_METRIC_UNSPECIFIED` to `COSINE`.
 * @type {Record<common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap], models.DistanceMetric>}
 * @internal
 */
const grpcToTsDistanceMetricMap: Record<common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap], models.DistanceMetric> = {
  [common_pb.DistanceMetric.COSINE]: models.DistanceMetric.COSINE,
  [common_pb.DistanceMetric.EUCLIDEAN_L2]: models.DistanceMetric.EUCLIDEAN_L2,
  [common_pb.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED]: models.DistanceMetric.COSINE, // Default
};

/**
 * Maps gRPC `CollectionStatus` enum to TypeScript `CollectionStatus` enum.
 * Defaults `COLLECTION_STATUS_UNSPECIFIED` to `GREEN`.
 * @type {Record<collections_service_pb.CollectionStatusMap[keyof collections_service_pb.CollectionStatusMap], models.CollectionStatus>}
 * @internal
 */
const grpcToTsCollectionStatusMap: Record<collections_service_pb.CollectionStatusMap[keyof collections_service_pb.CollectionStatusMap], models.CollectionStatus> = {
  [collections_service_pb.CollectionStatus.GREEN]: models.CollectionStatus.GREEN,
  [collections_service_pb.CollectionStatus.YELLOW]: models.CollectionStatus.YELLOW,
  [collections_service_pb.CollectionStatus.RED]: models.CollectionStatus.RED,
  [collections_service_pb.CollectionStatus.OPTIMIZING]: models.CollectionStatus.OPTIMIZING,
  [collections_service_pb.CollectionStatus.CREATING]: models.CollectionStatus.CREATING,
  [collections_service_pb.CollectionStatus.COLLECTION_STATUS_UNSPECIFIED]: models.CollectionStatus.GREEN, // Default
};

/**
 * Maps gRPC `StatusCode` enum to TypeScript `StatusCode` enum.
 * Defaults `STATUS_CODE_UNSPECIFIED` to `ERROR`.
 * @type {Record<common_pb.StatusCodeMap[keyof common_pb.StatusCodeMap], models.StatusCode>}
 * @internal
 */
const grpcToTsStatusCodeMap: Record<common_pb.StatusCodeMap[keyof common_pb.StatusCodeMap], models.StatusCode> = {
  [common_pb.StatusCode.OK]: models.StatusCode.OK,
  [common_pb.StatusCode.ERROR]: models.StatusCode.ERROR,
  [common_pb.StatusCode.NOT_FOUND]: models.StatusCode.NOT_FOUND,
  [common_pb.StatusCode.INVALID_ARGUMENT]: models.StatusCode.INVALID_ARGUMENT,
  [common_pb.StatusCode.STATUS_CODE_UNSPECIFIED]: models.StatusCode.ERROR, // Default
};


// --- Helper for google.protobuf.Value ---

/**
 * Converts a gRPC `google.protobuf.Value` to its TypeScript equivalent (`models.PayloadValue`).
 * Handles various protobuf value types like null, number, string, boolean, struct, and list.
 * @param {Value} grpcVal - The gRPC `Value` object to convert.
 * @returns {models.PayloadValue} The corresponding TypeScript value.
 * @internal
 */
export function grpcValueToTs(grpcVal: Value): models.PayloadValue {
  if (grpcVal.hasNullValue()) return null;
  if (grpcVal.hasNumberValue()) return grpcVal.getNumberValue();
  if (grpcVal.hasStringValue()) return grpcVal.getStringValue();
  if (grpcVal.hasBoolValue()) return grpcVal.getBoolValue();
  if (grpcVal.hasStructValue()) {
    const struct = grpcVal.getStructValue();
    if (!struct) return {}; // Should ideally not happen if hasStructValue is true
    const obj: { [key: string]: models.PayloadValue } = {};
    struct.getFieldsMap().forEach((value: Value, key: string) => {
      obj[key] = grpcValueToTs(value);
    });
    return obj;
  }
  if (grpcVal.hasListValue()) {
    const list = grpcVal.getListValue();
    if (!list) return []; // Should ideally not happen if hasListValue is true
    return list.getValuesList().map(grpcValueToTs);
  }
  // This case should ideally not be reached if the Value object is well-formed.
  // It implies a Value object that doesn't match any of the 'hasXValue' checks.
  return null;
}

/**
 * Converts a TypeScript `models.PayloadValue` to its gRPC `google.protobuf.Value` equivalent.
 * Handles various TypeScript value types like null, boolean, number, string, array, and object.
 * @param {models.PayloadValue} tsVal - The TypeScript value to convert.
 * @returns {Value} The corresponding gRPC `Value` object.
 * @internal
 */
export function tsValueToGrpc(tsVal: models.PayloadValue): Value {
  const grpcVal = new Value();
  if (tsVal === null) {
    grpcVal.setNullValue(0); // 0 corresponds to google.protobuf.NullValue.NULL_VALUE
  } else if (typeof tsVal === 'boolean') {
    grpcVal.setBoolValue(tsVal);
  } else if (typeof tsVal === 'number') {
    grpcVal.setNumberValue(tsVal);
  } else if (typeof tsVal === 'string') {
    grpcVal.setStringValue(tsVal);
  } else if (Array.isArray(tsVal)) {
    const listValue = new struct_pb.ListValue();
    listValue.setValuesList(tsVal.map(tsValueToGrpc));
    grpcVal.setListValue(listValue);
  } else if (typeof tsVal === 'object' && tsVal !== null) { // Check for null to avoid treating null as an object
    const structValue = new struct_pb.Struct();
    const fieldsMap = structValue.getFieldsMap();
    for (const key in tsVal) {
      // Ensure the key is an own property of the object
      if (Object.prototype.hasOwnProperty.call(tsVal, key)) {
        // Recursively convert the value associated with the key
        fieldsMap.set(key, tsValueToGrpc(tsVal[key]));
      }
    }
    grpcVal.setStructValue(structValue);
  } else {
    // Fallback for any other types (e.g., undefined, functions), convert to string.
    // This might indicate an issue with the input `tsVal` if it's not a valid PayloadValue.
    grpcVal.setStringValue(String(tsVal));
  }
  return grpcVal;
}

// --- Conversion Functions ---

/**
 * Converts a TypeScript `models.Vector` to a gRPC `common_pb.Vector`.
 * @param {models.Vector} vector - The TypeScript vector model.
 * @returns {common_pb.Vector} The gRPC vector object.
 * @internal
 */
export function tsToGrpcVector(vector: models.Vector): common_pb.Vector {
  const grpcVector = new common_pb.Vector();
  grpcVector.setElementsList(vector.elements);
  return grpcVector;
}

/**
 * Converts a gRPC `common_pb.Vector` to a TypeScript `models.Vector`.
 * @param {common_pb.Vector} vectorPb - The gRPC vector object.
 * @returns {models.Vector} The TypeScript vector model.
 * @internal
 */
export function grpcToTsVector(vectorPb: common_pb.Vector): models.Vector {
  return { elements: vectorPb.getElementsList() };
}

/**
 * Converts a TypeScript `models.Payload` to a gRPC `common_pb.Payload`.
 * @param {models.Payload} payload - The TypeScript payload model.
 * @returns {common_pb.Payload} The gRPC payload object.
 * @internal
 */
export function tsToGrpcPayload(payload: models.Payload): common_pb.Payload {
  const grpcPayload = new common_pb.Payload();
  const fieldsMap = grpcPayload.getFieldsMap();
  for (const key in payload.fields) {
    if (Object.prototype.hasOwnProperty.call(payload.fields, key)) {
      fieldsMap.set(key, tsValueToGrpc(payload.fields[key]));
    }
  }
  return grpcPayload;
}

/**
 * Converts a gRPC `common_pb.Payload` to a TypeScript `models.Payload`.
 * @param {common_pb.Payload} payloadPb - The gRPC payload object.
 * @returns {models.Payload} The TypeScript payload model.
 * @internal
 */
export function grpcToTsPayload(payloadPb: common_pb.Payload): models.Payload {
  const fields: { [key: string]: models.PayloadValue } = {};
  payloadPb.getFieldsMap().forEach((value: Value, key: string) => {
    fields[key] = grpcValueToTs(value);
  });
  return { fields };
}

/**
 * Converts a TypeScript `models.PointStruct` to a gRPC `common_pb.PointStruct`.
 * @param {models.PointStruct} point - The TypeScript point structure model.
 * @returns {common_pb.PointStruct} The gRPC point structure object.
 * @internal
 */
export function tsToGrpcPointStruct(point: models.PointStruct): common_pb.PointStruct {
  const grpcPoint = new common_pb.PointStruct();
  grpcPoint.setId(point.id);
  grpcPoint.setVector(tsToGrpcVector(point.vector));
  if (point.payload) {
    grpcPoint.setPayload(tsToGrpcPayload(point.payload));
  }
  // Version is not set from TS model to gRPC as it's typically server-assigned.
  return grpcPoint;
}

/**
 * Converts a gRPC `common_pb.PointStruct` to a TypeScript `models.PointStruct`.
 * Assumes that if `hasVector()` is true, `getVector()` will return a non-null object.
 * @param {common_pb.PointStruct} pointPb - The gRPC point structure object.
 * @returns {models.PointStruct} The TypeScript point structure model.
 * @internal
 */
export function grpcToTsPointStruct(pointPb: common_pb.PointStruct): models.PointStruct {
  const vector = pointPb.hasVector() ? grpcToTsVector(pointPb.getVector()!) : undefined;
  if (!vector) {
    // This case should ideally be handled based on how the application expects missing vectors.
    // For now, throwing an error or returning a PointStruct with an undefined vector might be options.
    // However, the current models.PointStruct requires a vector.
    // console.warn(`gRPC PointStruct with ID ${pointPb.getId()} is missing a vector.`);
  }
  return {
    id: pointPb.getId(),
    vector: vector!, // Asserting vector is present as per current models.PointStruct definition.
    payload: pointPb.hasPayload() ? grpcToTsPayload(pointPb.getPayload()!) : null,
    // version: pointPb.hasVersion() ? pointPb.getVersion() : null, // Removed as models.PointStruct does not have version
  };
}

/**
 * Converts a gRPC `common_pb.ScoredPoint` to a TypeScript `models.ScoredPoint`.
 * @param {common_pb.ScoredPoint} scoredPointPb - The gRPC scored point object.
 * @returns {models.ScoredPoint} The TypeScript scored point model.
 * @internal
 */
export function grpcToTsScoredPoint(scoredPointPb: common_pb.ScoredPoint): models.ScoredPoint {
  const vector = scoredPointPb.hasVector() ? grpcToTsVector(scoredPointPb.getVector()!) : null;
  // No assertion for vector here as ScoredPoint model allows vector to be null.
  return {
    id: scoredPointPb.getId(),
    vector: vector,
    payload: scoredPointPb.hasPayload() ? grpcToTsPayload(scoredPointPb.getPayload()!) : null,
    score: scoredPointPb.getScore(),
    version: scoredPointPb.hasVersion() ? scoredPointPb.getVersion() : null,
  };
}

/**
 * Converts a TypeScript `models.HnswConfigParams` to a gRPC `common_pb.HnswConfigParams`.
 * @param {models.HnswConfigParams} config - The TypeScript HNSW configuration parameters model.
 * @returns {common_pb.HnswConfigParams} The gRPC HNSW configuration parameters object.
 * @internal
 */
export function tsToGrpcHnswConfigParams(config: models.HnswConfigParams): common_pb.HnswConfigParams {
  const grpcConfig = new common_pb.HnswConfigParams();
  grpcConfig.setM(config.m);
  grpcConfig.setEfConstruction(config.efConstruction);
  grpcConfig.setEfSearch(config.efSearch);
  grpcConfig.setMl(config.ml);
  grpcConfig.setVectorDim(config.vectorDim);
  grpcConfig.setMMax0(config.mMax0); // Ensure mMax0 is included
  if (config.seed !== undefined && config.seed !== null) {
    grpcConfig.setSeed(config.seed);
  }
  return grpcConfig;
}

/**
 * Converts a gRPC `common_pb.HnswConfigParams` to a TypeScript `models.HnswConfigParams`.
 * @param {common_pb.HnswConfigParams} configPb - The gRPC HNSW configuration parameters object.
 * @returns {models.HnswConfigParams} The TypeScript HNSW configuration parameters model.
 * @internal
 */
export function grpcToTsHnswConfigParams(configPb: common_pb.HnswConfigParams): models.HnswConfigParams {
  return {
    m: configPb.getM(),
    efConstruction: configPb.getEfConstruction(),
    efSearch: configPb.getEfSearch(),
    ml: configPb.getMl(),
    seed: configPb.hasSeed() ? configPb.getSeed() : null,
    vectorDim: configPb.getVectorDim(),
    mMax0: configPb.getMMax0(), // Ensure mMax0 is included
  };
}

/**
 * Converts a TypeScript `models.DistanceMetric` enum to its gRPC `DistanceMetric` enum equivalent.
 * Defaults to `DISTANCE_METRIC_UNSPECIFIED` if the mapping is not found.
 * @param {models.DistanceMetric} metric - The TypeScript distance metric enum.
 * @returns {common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap]} The gRPC distance metric enum value.
 * @internal
 */
export function tsToGrpcDistanceMetric(metric: models.DistanceMetric): common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap] {
  return tsToGrpcDistanceMetricMap[metric] || common_pb.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED;
}

/**
 * Converts a gRPC `DistanceMetric` enum to its TypeScript `models.DistanceMetric` enum equivalent.
 * Defaults to `models.DistanceMetric.COSINE` if the mapping is not found or if unspecified.
 * @param {common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap]} metricPb - The gRPC distance metric enum value.
 * @returns {models.DistanceMetric} The TypeScript distance metric enum.
 * @internal
 */
export function grpcToTsDistanceMetric(metricPb: common_pb.DistanceMetricMap[keyof common_pb.DistanceMetricMap]): models.DistanceMetric {
  return grpcToTsDistanceMetricMap[metricPb] || models.DistanceMetric.COSINE;
}

/**
 * Converts a gRPC `CollectionStatus` enum to its TypeScript `models.CollectionStatus` enum equivalent.
 * Defaults to `models.CollectionStatus.GREEN` if the mapping is not found or if unspecified.
 * @param {collections_service_pb.CollectionStatusMap[keyof collections_service_pb.CollectionStatusMap]} statusPb - The gRPC collection status enum value.
 * @returns {models.CollectionStatus} The TypeScript collection status enum.
 * @internal
 */
export function grpcToTsCollectionStatus(statusPb: collections_service_pb.CollectionStatusMap[keyof collections_service_pb.CollectionStatusMap]): models.CollectionStatus {
  return grpcToTsCollectionStatusMap[statusPb] || models.CollectionStatus.GREEN;
}

/**
 * Converts a gRPC `GetCollectionInfoResponse` to a TypeScript `models.CollectionInfo`.
 * Assumes that if `getConfig()` is called, it will return a non-null object if `hasConfig()` would be true (though `hasConfig` might not exist for message fields).
 * @param {collections_service_pb.GetCollectionInfoResponse} infoPb - The gRPC collection info response object.
 * @returns {models.CollectionInfo} The TypeScript collection info model.
 * @internal
 */
export function grpcToTsCollectionInfo(infoPb: collections_service_pb.GetCollectionInfoResponse): models.CollectionInfo {
  const config = infoPb.getConfig(); // getConfig() should exist
  if (!config) {
    // This case should ideally not happen if the server sends a valid response.
    // console.warn(`GetCollectionInfoResponse for ${infoPb.getCollectionName()} is missing HNSW config.`);
  }
  return {
    collectionName: infoPb.getCollectionName(),
    status: grpcToTsCollectionStatus(infoPb.getStatus()),
    vectorCount: infoPb.getVectorCount(),
    segmentCount: infoPb.getSegmentCount(), // Assuming this field exists or defaults appropriately
    diskSizeBytes: infoPb.getDiskSizeBytes(),
    ramFootprintBytes: infoPb.getRamFootprintBytes(),
    config: config ? grpcToTsHnswConfigParams(config) : {} as models.HnswConfigParams, // Provide a default if config is missing
    distanceMetric: grpcToTsDistanceMetric(infoPb.getDistanceMetric()),
  };
}

/**
 * Converts a gRPC `CollectionDescription` to a TypeScript `models.CollectionDescription`.
 * @param {collections_service_pb.CollectionDescription} descPb - The gRPC collection description object.
 * @returns {models.CollectionDescription} The TypeScript collection description model.
 * @internal
 */
export function grpcToTsCollectionDescription(descPb: collections_service_pb.CollectionDescription): models.CollectionDescription {
  return {
    name: descPb.getName(),
    vectorCount: descPb.getVectorCount(),
    status: grpcToTsCollectionStatus(descPb.getStatus()),
    dimensions: descPb.getDimensions(),
    distanceMetric: grpcToTsDistanceMetric(descPb.getDistanceMetric()),
  };
}

/**
 * Converts a TypeScript `models.Filter` to a gRPC `common_pb.Filter`.
 * If the filter is null, undefined, or its `mustMatchExact` field is empty, returns undefined.
 * @param {models.Filter | null | undefined} filter - The TypeScript filter model.
 * @returns {common_pb.Filter | undefined} The gRPC filter object, or undefined if the input filter is effectively empty.
 * @internal
 */
export function tsToGrpcFilter(filter?: models.Filter | null): common_pb.Filter | undefined {
  if (!filter || !filter.mustMatchExact || Object.keys(filter.mustMatchExact).length === 0) {
    return undefined;
  }
  const grpcFilter = new common_pb.Filter();
  const fieldsMap = grpcFilter.getMustMatchExactMap();
  for (const key in filter.mustMatchExact) {
    if (Object.prototype.hasOwnProperty.call(filter.mustMatchExact, key)) {
      fieldsMap.set(key, tsValueToGrpc(filter.mustMatchExact[key]));
    }
  }
  return grpcFilter;
}

/**
 * Converts a gRPC `common_pb.Filter` to a TypeScript `models.Filter`.
 * If the gRPC filter is null, undefined, or its `must_match_exact` map is empty, returns null.
 * @param {common_pb.Filter | null | undefined} filterPb - The gRPC filter object.
 * @returns {models.Filter | null} The TypeScript filter model, or null if the input filter is effectively empty.
 * @internal
 */
export function grpcToTsFilter(filterPb?: common_pb.Filter | null): models.Filter | null {
    // For map fields in google-protobuf, there isn't a 'hasFieldName' method.
    // We check if the map itself is empty or if the filterPb is null/undefined.
    if (!filterPb) {
        return null;
    }
    const mustMatchExactMap = filterPb.getMustMatchExactMap();
    if (mustMatchExactMap.getLength() === 0) {
        return null;
    }
    const fields: { [key: string]: models.PayloadValue } = {};
    mustMatchExactMap.forEach((value: Value, key: string) => {
        fields[key] = grpcValueToTs(value);
    });
    return { mustMatchExact: fields };
}

/**
 * Converts a TypeScript `models.SearchParams` to a gRPC `common_pb.SearchParams`.
 * If the params object is null or undefined, returns undefined.
 * Currently maps `hnsw_ef` from the TS model to `ef_search` in the gRPC model.
 * @param {models.SearchParams | null | undefined} params - The TypeScript search parameters model.
 * @returns {common_pb.SearchParams | undefined} The gRPC search parameters object, or undefined if input is null/undefined.
 * @internal
 */
export function tsToGrpcSearchParams(params?: models.SearchParams | null): common_pb.SearchParams | undefined {
  if (!params) {
    return undefined;
  }
  // SearchParams is defined in common.proto and has 'ef_search' field.
  const grpcParams = new common_pb.SearchParams();

  if (params.hnsw_ef !== undefined && params.hnsw_ef !== null) {
    // The proto field is 'ef_search', so the method should be 'setEfSearch'.
    // We are mapping the TS model's 'hnsw_ef' to this.
    if (typeof grpcParams.setEfSearch === 'function') {
        grpcParams.setEfSearch(params.hnsw_ef);
    } else {
        // This case indicates a mismatch between assumption and generated code.
        // For safety, one might log a warning or error if the method doesn't exist.
        // console.warn("SearchParams.setEfSearch method not found on generated gRPC type common_pb.SearchParams.");
    }
  }
  // The 'exact' field from models.SearchParams is not currently mapped as it's not in common_pb.SearchParams.
  // If common_pb.SearchParams is updated to include 'exact', this function should be updated.

  return grpcParams;
}

/**
 * Converts a gRPC `StatusCode` enum to its TypeScript `models.StatusCode` enum equivalent.
 * Defaults to `models.StatusCode.ERROR` if the mapping is not found or if unspecified.
 * @param {common_pb.StatusCodeMap[keyof common_pb.StatusCodeMap]} statusCodePb - The gRPC status code enum value.
 * @returns {models.StatusCode} The TypeScript status code enum.
 * @internal
 */
export function grpcToTsStatusCode(statusCodePb: common_pb.StatusCodeMap[keyof common_pb.StatusCodeMap]): models.StatusCode {
    return grpcToTsStatusCodeMap[statusCodePb] || models.StatusCode.ERROR;
}

/**
 * Converts a gRPC `common_pb.PointOperationStatus` to a TypeScript `models.PointOperationStatus`.
 * @param {common_pb.PointOperationStatus} statusPb - The gRPC point operation status object.
 * @returns {models.PointOperationStatus} The TypeScript point operation status model.
 * @internal
 */
export function grpcToTsPointOperationStatus(statusPb: common_pb.PointOperationStatus): models.PointOperationStatus {
    return {
        pointId: statusPb.getPointId(),
        statusCode: grpcToTsStatusCode(statusPb.getStatusCode()),
        errorMessage: statusPb.hasErrorMessage() ? statusPb.getErrorMessage() : null,
    };
}
