/**
 * Unit tests for TypeScript conversion functions in src/conversions.ts
 */
import * as models from '../src/models';
import * as conversions from '../src/conversions';
import * as common_pb from '../src/_grpc/vortex/api/v1/common_pb';
import * as collections_service_pb from '../src/_grpc/vortex/api/v1/collections_service_pb';
import { Value } from 'google-protobuf/google/protobuf/struct_pb';
import * as struct_pb from 'google-protobuf/google/protobuf/struct_pb';


// Helper to compare payloads due to potential nested objects and float precision
function expectPayloadsToEqual(p1: models.Payload | null | undefined, p2: models.Payload | null | undefined) {
    if (p1 === null || p1 === undefined) {
        // Check if p2 is either null or undefined
        expect(p2 === null || p2 === undefined).toBe(true);
        return;
    }
    expect(p2).toBeDefined();
    expect(p2).not.toBeNull();

    const p1Fields = p1!.fields;
    const p2Fields = p2!.fields;

    expect(Object.keys(p1Fields).length).toEqual(Object.keys(p2Fields).length);

    for (const key in p1Fields) {
        expect(p2Fields).toHaveProperty(key);
        const v1 = p1Fields[key];
        const v2 = p2Fields[key];
        if (typeof v1 === 'number' && typeof v2 === 'number') {
            expect(v1).toBeCloseTo(v2);
        } else if (Array.isArray(v1) && Array.isArray(v2)) {
            // Basic array comparison, might need deep comparison for nested arrays of objects
            expect(v1.length).toEqual(v2.length);
            for(let i=0; i < v1.length; i++) {
                if (typeof v1[i] === 'number' && typeof v2[i] === 'number') {
                     expect(v1[i]).toBeCloseTo(v2[i] as number);
                } else {
                    expect(v1[i]).toEqual(v2[i]);
                }
            }
        } else if (typeof v1 === 'object' && v1 !== null && typeof v2 === 'object' && v2 !== null) {
             // Basic object comparison, might need deep recursive for nested objects
            expect(v1).toEqual(v2); // This might fail for floats inside nested objects
        }
        else {
            expect(v1).toEqual(v2);
        }
    }
}


describe('TypeScript Conversions', () => {
  const sampleTsVector: models.Vector = { elements: [1.1, 2.2, 3.3] };
  const sampleGrpcVector = new common_pb.Vector();
  sampleGrpcVector.setElementsList([1.1, 2.2, 3.3]);

  const sampleTsHnswConfig: models.HnswConfigParams = { m: 16, efConstruction: 200, efSearch: 100, ml: 0.5, vectorDim: 3, mMax0: 32, seed: 42 };
  const sampleGrpcHnswConfig = new common_pb.HnswConfigParams();
  sampleGrpcHnswConfig.setM(16);
  sampleGrpcHnswConfig.setEfConstruction(200);
  sampleGrpcHnswConfig.setEfSearch(100);
  sampleGrpcHnswConfig.setMl(0.5);
  sampleGrpcHnswConfig.setVectorDim(3);
  sampleGrpcHnswConfig.setMMax0(32);
  sampleGrpcHnswConfig.setSeed(42);


  const sampleTsPayload: models.Payload = {
    fields: {
      string_field: "hello", int_field: 123, float_field: 45.67,
      bool_field: true, null_field: null,
      list_field: ["a", 1, false, null, { nested_in_list: 0.5 }],
      struct_field: { nested_str: "world", nested_int: 789, nested_float: 0.123 }
    }
  };
  
  const sampleGrpcPayload = new common_pb.Payload();
  const fieldsMap = sampleGrpcPayload.getFieldsMap();
  fieldsMap.set("string_field", conversions.tsValueToGrpc("hello"));
  fieldsMap.set("int_field", conversions.tsValueToGrpc(123));
  fieldsMap.set("float_field", conversions.tsValueToGrpc(45.67));
  fieldsMap.set("bool_field", conversions.tsValueToGrpc(true));
  fieldsMap.set("null_field", conversions.tsValueToGrpc(null));
  const listVal = new struct_pb.ListValue();
  listVal.setValuesList([
    conversions.tsValueToGrpc("a"), conversions.tsValueToGrpc(1),
    conversions.tsValueToGrpc(false), conversions.tsValueToGrpc(null),
    conversions.tsValueToGrpc({ nested_in_list: 0.5 })
  ]);
  const listValueProto = new Value();
  listValueProto.setListValue(listVal);
  fieldsMap.set("list_field", listValueProto);

  const structVal = new struct_pb.Struct();
  const structMap = structVal.getFieldsMap();
  structMap.set("nested_str", conversions.tsValueToGrpc("world"));
  structMap.set("nested_int", conversions.tsValueToGrpc(789));
  structMap.set("nested_float", conversions.tsValueToGrpc(0.123));
  const structValueProto = new Value();
  structValueProto.setStructValue(structVal);
  fieldsMap.set("struct_field", structValueProto);


  test('tsValueToGrpc and grpcValueToTs', () => {
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc("string"))).toBe("string");
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc(123.45))).toBeCloseTo(123.45);
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc(true))).toBe(true);
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc(null))).toBeNull();
    
    const list = ["a", 1, true, null];
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc(list))).toEqual(list);
    
    const obj = { a: "b", c: 1, d: { e: null } };
    expect(conversions.grpcValueToTs(conversions.tsValueToGrpc(obj))).toEqual(obj);
  });

  test('Vector conversion', () => {
    const grpcVec = conversions.tsToGrpcVector(sampleTsVector);
    expect(grpcVec.getElementsList()).toEqual(expect.arrayContaining([expect.closeTo(1.1), expect.closeTo(2.2), expect.closeTo(3.3)]));
    const tsVec = conversions.grpcToTsVector(sampleGrpcVector);
    expect(tsVec.elements).toEqual(expect.arrayContaining([expect.closeTo(1.1), expect.closeTo(2.2), expect.closeTo(3.3)]));
  });

  test('Payload conversion', () => {
    const grpcPayload = conversions.tsToGrpcPayload(sampleTsPayload);
    expect(grpcPayload.getFieldsMap().get("string_field")?.getStringValue()).toBe("hello");
    expect(grpcPayload.getFieldsMap().get("float_field")?.getNumberValue()).toBeCloseTo(45.67);

    const tsPayload = conversions.grpcToTsPayload(sampleGrpcPayload);
    expectPayloadsToEqual(tsPayload, sampleTsPayload);
  });

  test('PointStruct conversion', () => {
    const tsPoint: models.PointStruct = { id: "p1", vector: sampleTsVector, payload: sampleTsPayload };
    const grpcPoint = conversions.tsToGrpcPointStruct(tsPoint);
    expect(grpcPoint.getId()).toBe("p1");
    expect(grpcPoint.getVector()?.getElementsList()).toEqual(expect.arrayContaining([expect.closeTo(1.1)])); // Check one element
    
    const tsPointBack = conversions.grpcToTsPointStruct(grpcPoint);
    expect(tsPointBack.id).toBe(tsPoint.id);
    expect(tsPointBack.vector.elements).toEqual(expect.arrayContaining([expect.closeTo(1.1)]));
    expectPayloadsToEqual(tsPointBack.payload, tsPoint.payload);
  });

  test('HnswConfigParams conversion', () => {
    const tsConfig: models.HnswConfigParams = { m: 16, efConstruction: 200, efSearch: 100, ml: 0.5, vectorDim: 3, mMax0: 32, seed: 42 };
    const grpcConfig = conversions.tsToGrpcHnswConfigParams(tsConfig);
    expect(grpcConfig.getM()).toBe(16);
    expect(grpcConfig.getSeed()).toBe(42);

    const tsConfigBack = conversions.grpcToTsHnswConfigParams(grpcConfig);
    expect(tsConfigBack).toEqual(tsConfig);
  });

  test('Enum conversions', () => {
    expect(conversions.tsToGrpcDistanceMetric(models.DistanceMetric.COSINE)).toBe(common_pb.DistanceMetric.COSINE);
    expect(conversions.grpcToTsDistanceMetric(common_pb.DistanceMetric.EUCLIDEAN_L2)).toBe(models.DistanceMetric.EUCLIDEAN_L2);
    // Test default/unspecified for DistanceMetric
    expect(conversions.grpcToTsDistanceMetric(common_pb.DistanceMetric.DISTANCE_METRIC_UNSPECIFIED)).toBe(models.DistanceMetric.COSINE);
  });

  test('CollectionStatus enum conversion', () => {
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.GREEN)).toBe(models.CollectionStatus.GREEN);
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.YELLOW)).toBe(models.CollectionStatus.YELLOW);
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.RED)).toBe(models.CollectionStatus.RED);
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.OPTIMIZING)).toBe(models.CollectionStatus.OPTIMIZING);
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.CREATING)).toBe(models.CollectionStatus.CREATING);
    expect(conversions.grpcToTsCollectionStatus(collections_service_pb.CollectionStatus.COLLECTION_STATUS_UNSPECIFIED)).toBe(models.CollectionStatus.GREEN);
  });

  test('StatusCode enum conversion', () => {
    expect(conversions.grpcToTsStatusCode(common_pb.StatusCode.OK)).toBe(models.StatusCode.OK);
    expect(conversions.grpcToTsStatusCode(common_pb.StatusCode.ERROR)).toBe(models.StatusCode.ERROR);
    expect(conversions.grpcToTsStatusCode(common_pb.StatusCode.NOT_FOUND)).toBe(models.StatusCode.NOT_FOUND);
    expect(conversions.grpcToTsStatusCode(common_pb.StatusCode.INVALID_ARGUMENT)).toBe(models.StatusCode.INVALID_ARGUMENT);
    expect(conversions.grpcToTsStatusCode(common_pb.StatusCode.STATUS_CODE_UNSPECIFIED)).toBe(models.StatusCode.ERROR);
  });
  
  test('ScoredPoint conversion', () => {
    const grpcScoredPoint = new common_pb.ScoredPoint();
    grpcScoredPoint.setId("sp1");
    grpcScoredPoint.setVector(sampleGrpcVector);
    grpcScoredPoint.setPayload(sampleGrpcPayload);
    grpcScoredPoint.setScore(0.99);
    grpcScoredPoint.setVersion(123);

    const tsScoredPoint = conversions.grpcToTsScoredPoint(grpcScoredPoint);
    expect(tsScoredPoint.id).toBe("sp1");
    expect(tsScoredPoint.vector).toEqual(sampleTsVector);
    expectPayloadsToEqual(tsScoredPoint.payload, sampleTsPayload);
    expect(tsScoredPoint.score).toBe(0.99);
    expect(tsScoredPoint.version).toBe(123);

    // Test with optional fields missing
    const minimalGrpcScoredPoint = new common_pb.ScoredPoint();
    minimalGrpcScoredPoint.setId("sp2");
    minimalGrpcScoredPoint.setScore(0.88);
    const minimalTsScoredPoint = conversions.grpcToTsScoredPoint(minimalGrpcScoredPoint);
    expect(minimalTsScoredPoint.id).toBe("sp2");
    expect(minimalTsScoredPoint.score).toBe(0.88);
    expect(minimalTsScoredPoint.vector).toBeNull();
    expect(minimalTsScoredPoint.payload).toBeNull();
    expect(minimalTsScoredPoint.version).toBeNull();
  });

  test('CollectionInfo conversion', () => {
    const grpcCollectionInfo = new collections_service_pb.GetCollectionInfoResponse(); // Using this as it matches the structure
    grpcCollectionInfo.setCollectionName("test_coll_info");
    grpcCollectionInfo.setStatus(collections_service_pb.CollectionStatus.OPTIMIZING);
    grpcCollectionInfo.setVectorCount(1000);
    grpcCollectionInfo.setSegmentCount(5);
    grpcCollectionInfo.setDiskSizeBytes(2048000);
    grpcCollectionInfo.setRamFootprintBytes(1024000);
    grpcCollectionInfo.setConfig(sampleGrpcHnswConfig);
    grpcCollectionInfo.setDistanceMetric(common_pb.DistanceMetric.EUCLIDEAN_L2);

    const tsCollectionInfo = conversions.grpcToTsCollectionInfo(grpcCollectionInfo);
    expect(tsCollectionInfo.collectionName).toBe("test_coll_info");
    expect(tsCollectionInfo.status).toBe(models.CollectionStatus.OPTIMIZING);
    expect(tsCollectionInfo.vectorCount).toBe(1000);
    expect(tsCollectionInfo.segmentCount).toBe(5);
    expect(tsCollectionInfo.diskSizeBytes).toBe(2048000);
    expect(tsCollectionInfo.ramFootprintBytes).toBe(1024000);
    expect(tsCollectionInfo.config).toEqual(sampleTsHnswConfig);
    expect(tsCollectionInfo.distanceMetric).toBe(models.DistanceMetric.EUCLIDEAN_L2);
  });

  test('CollectionDescription conversion', () => {
    const grpcCollectionDesc = new collections_service_pb.CollectionDescription(); // Using this as it matches the structure
    grpcCollectionDesc.setName("desc_coll_test");
    grpcCollectionDesc.setVectorCount(500);
    grpcCollectionDesc.setStatus(collections_service_pb.CollectionStatus.YELLOW);
    grpcCollectionDesc.setDimensions(64);
    grpcCollectionDesc.setDistanceMetric(common_pb.DistanceMetric.COSINE);

    const tsCollectionDesc = conversions.grpcToTsCollectionDescription(grpcCollectionDesc);
    expect(tsCollectionDesc.name).toBe("desc_coll_test");
    expect(tsCollectionDesc.vectorCount).toBe(500);
    expect(tsCollectionDesc.status).toBe(models.CollectionStatus.YELLOW);
    expect(tsCollectionDesc.dimensions).toBe(64);
    expect(tsCollectionDesc.distanceMetric).toBe(models.DistanceMetric.COSINE);
  });

  test('Filter conversion (tsToGrpcFilter and grpcToTsFilter)', () => {
    const tsFilter: models.Filter = { mustMatchExact: { color: "blue", count: 10 } };
    const grpcFilter = conversions.tsToGrpcFilter(tsFilter);
    expect(grpcFilter).toBeInstanceOf(common_pb.Filter);
    expect(grpcFilter?.getMustMatchExactMap().get("color")?.getStringValue()).toBe("blue");
    expect(grpcFilter?.getMustMatchExactMap().get("count")?.getNumberValue()).toBe(10);

    const tsFilterBack = conversions.grpcToTsFilter(grpcFilter);
    expect(tsFilterBack).toEqual(tsFilter);

    // Test empty/null cases for tsToGrpcFilter
    expect(conversions.tsToGrpcFilter(null)).toBeUndefined();
    expect(conversions.tsToGrpcFilter(undefined)).toBeUndefined();
    expect(conversions.tsToGrpcFilter({})).toBeUndefined();
    expect(conversions.tsToGrpcFilter({ mustMatchExact: {} })).toBeUndefined();
    expect(conversions.tsToGrpcFilter({ mustMatchExact: null })).toBeUndefined();


    // Test empty/null cases for grpcToTsFilter
    expect(conversions.grpcToTsFilter(null)).toBeNull();
    expect(conversions.grpcToTsFilter(undefined)).toBeNull();
    const emptyGrpcFilter = new common_pb.Filter();
    expect(conversions.grpcToTsFilter(emptyGrpcFilter)).toBeNull();
  });
  
  test('PointOperationStatus conversion', () => {
    const grpcStatusOk = new common_pb.PointOperationStatus();
    grpcStatusOk.setPointId("p1_ok");
    grpcStatusOk.setStatusCode(common_pb.StatusCode.OK);

    const tsStatusOk = conversions.grpcToTsPointOperationStatus(grpcStatusOk);
    expect(tsStatusOk.pointId).toBe("p1_ok");
    expect(tsStatusOk.statusCode).toBe(models.StatusCode.OK);
    expect(tsStatusOk.errorMessage).toBeNull();

    const grpcStatusErr = new common_pb.PointOperationStatus();
    grpcStatusErr.setPointId("p2_err");
    grpcStatusErr.setStatusCode(common_pb.StatusCode.ERROR);
    grpcStatusErr.setErrorMessage("Failed hard");

    const tsStatusErr = conversions.grpcToTsPointOperationStatus(grpcStatusErr);
    expect(tsStatusErr.pointId).toBe("p2_err");
    expect(tsStatusErr.statusCode).toBe(models.StatusCode.ERROR);
    expect(tsStatusErr.errorMessage).toBe("Failed hard");
  });

  describe('SearchParams conversion', () => {
    test('tsToGrpcSearchParams with undefined or null input', () => {
      expect(conversions.tsToGrpcSearchParams(undefined)).toBeUndefined();
      expect(conversions.tsToGrpcSearchParams(null)).toBeUndefined();
    });

    test('tsToGrpcSearchParams with empty SearchParams', () => {
      const tsParams: models.SearchParams = {};
      const grpcParams = conversions.tsToGrpcSearchParams(tsParams);
      expect(grpcParams).toBeInstanceOf(common_pb.SearchParams);
      // Check that optional fields are not set or have default values
      // For proto3, unset optional scalar fields read as the type's default (0 for numbers, false for bools)
      // The hasFieldName() methods are not generated for proto3 optional scalar fields by default with protoc-gen-ts.
      // We rely on the generated getter returning the default if not set.
      expect(grpcParams?.getEfSearch()).toBe(0); // Default for uint32 if not explicitly set
    });

    test('tsToGrpcSearchParams with hnsw_ef set', () => {
      const tsParams: models.SearchParams = { hnsw_ef: 120 };
      const grpcParams = conversions.tsToGrpcSearchParams(tsParams);
      expect(grpcParams).toBeInstanceOf(common_pb.SearchParams);
      expect(grpcParams?.getEfSearch()).toBe(120);
    });

    test('tsToGrpcSearchParams with hnsw_ef set to 0', () => {
      const tsParams: models.SearchParams = { hnsw_ef: 0 };
      const grpcParams = conversions.tsToGrpcSearchParams(tsParams);
      expect(grpcParams).toBeInstanceOf(common_pb.SearchParams);
      expect(grpcParams?.getEfSearch()).toBe(0);
    });
  });
});
