/**
 * Unit tests for TypeScript models in src/models.ts
 */
import * as models from '../src/models';

describe('TypeScript Models', () => {
  test('Vector interface', () => {
    const vec: models.Vector = { elements: [1, 2, 3] };
    expect(vec.elements).toEqual([1, 2, 3]);
  });

  test('Payload interface with various types', () => {
    const payload: models.Payload = {
      fields: {
        name: "test_item",
        count: 10,
        price: 19.99,
        isActive: true,
        tags: ["tag1", "tag2"],
        metadata: { key: "value", nested_key: { sub_key: 100 } },
        nullableField: null,
      }
    };
    expect(payload.fields.name).toBe("test_item");
    expect(payload.fields.count).toBe(10);
    expect(payload.fields.price).toBe(19.99);
    expect(payload.fields.isActive).toBe(true);
    expect(payload.fields.tags).toEqual(["tag1", "tag2"]);
    expect((payload.fields.metadata as any).key).toBe("value");
    expect(payload.fields.nullableField).toBeNull();
  });

  test('PointStruct interface', () => {
    const point: models.PointStruct = {
      id: "point1",
      vector: { elements: [0.1, 0.2] },
      payload: { fields: { category: "A" } }
    };
    expect(point.id).toBe("point1");
    expect(point.vector.elements).toEqual([0.1, 0.2]);
    expect(point.payload?.fields.category).toBe("A");

    const pointNoPayload: models.PointStruct = {
      id: "point2",
      vector: { elements: [0.3, 0.4] }
    };
    expect(pointNoPayload.payload).toBeUndefined();
  });

  test('HnswConfigParams interface', () => {
    const config: models.HnswConfigParams = {
      m: 16,
      efConstruction: 200,
      efSearch: 100,
      ml: 0.5,
      vectorDim: 128,
      mMax0: 32,
      seed: 42
    };
    expect(config.m).toBe(16);
    expect(config.seed).toBe(42);
  });

  test('DistanceMetric enum', () => {
    expect(models.DistanceMetric.COSINE).toBe("COSINE");
    expect(models.DistanceMetric.EUCLIDEAN_L2).toBe("EUCLIDEAN_L2");
  });

  test('CollectionStatus enum', () => {
    expect(models.CollectionStatus.GREEN).toBe("GREEN");
    expect(models.CollectionStatus.YELLOW).toBe("YELLOW");
    expect(models.CollectionStatus.RED).toBe("RED");
    expect(models.CollectionStatus.OPTIMIZING).toBe("OPTIMIZING");
    expect(models.CollectionStatus.CREATING).toBe("CREATING");
  });

  test('StatusCode enum', () => {
    expect(models.StatusCode.OK).toBe("OK");
    expect(models.StatusCode.ERROR).toBe("ERROR");
    expect(models.StatusCode.NOT_FOUND).toBe("NOT_FOUND");
    expect(models.StatusCode.INVALID_ARGUMENT).toBe("INVALID_ARGUMENT");
  });

  test('ScoredPoint interface', () => {
    const scoredPoint: models.ScoredPoint = {
      id: "sp1",
      vector: { elements: [0.5, 0.6] },
      payload: { fields: { type: "scored" } },
      score: 0.99,
      version: 123
    };
    expect(scoredPoint.id).toBe("sp1");
    expect(scoredPoint.score).toBe(0.99);
    expect(scoredPoint.version).toBe(123);
    expect(scoredPoint.payload?.fields.type).toBe("scored");

    const scoredPointMinimal: models.ScoredPoint = {
      id: "sp2",
      score: 0.88,
    };
    expect(scoredPointMinimal.vector).toBeUndefined();
    expect(scoredPointMinimal.payload).toBeUndefined();
    expect(scoredPointMinimal.version).toBeUndefined();
  });

  test('Filter interface', () => {
    const filter: models.Filter = {
      mustMatchExact: {
        color: "blue",
        count: 5
      }
    };
    expect(filter.mustMatchExact?.color).toBe("blue");
    expect((filter.mustMatchExact?.count as number)).toBe(5);

    const emptyFilter: models.Filter = {};
    expect(emptyFilter.mustMatchExact).toBeUndefined();
  });

  test('PointOperationStatus interface', () => {
    const status: models.PointOperationStatus = {
      pointId: "p1",
      statusCode: models.StatusCode.OK,
      errorMessage: null
    };
    expect(status.pointId).toBe("p1");
    expect(status.statusCode).toBe(models.StatusCode.OK);
    expect(status.errorMessage).toBeNull();

    const errorStatus: models.PointOperationStatus = {
      pointId: "p2",
      statusCode: models.StatusCode.ERROR,
      errorMessage: "Failed to process"
    };
    expect(errorStatus.errorMessage).toBe("Failed to process");
  });

  test('SearchParams interface', () => {
    const params: models.SearchParams = {
      hnsw_ef: 120
    };
    expect(params.hnsw_ef).toBe(120);

    const emptyParams: models.SearchParams = {};
    expect(emptyParams.hnsw_ef).toBeUndefined();
  });

  test('CollectionInfo interface', () => {
    const collectionInfo: models.CollectionInfo = {
      collectionName: "test_coll",
      status: models.CollectionStatus.GREEN,
      vectorCount: 1000,
      segmentCount: 2,
      diskSizeBytes: 1024000,
      ramFootprintBytes: 512000,
      config: { m: 16, efConstruction: 200, efSearch: 100, ml: 0.5, vectorDim: 128, mMax0: 32 },
      distanceMetric: models.DistanceMetric.COSINE
    };
    expect(collectionInfo.collectionName).toBe("test_coll");
    expect(collectionInfo.status).toBe(models.CollectionStatus.GREEN);
    expect(collectionInfo.vectorCount).toBe(1000);
    expect(collectionInfo.config.m).toBe(16);
  });

  test('CollectionDescription interface', () => {
    const desc: models.CollectionDescription = {
      name: "desc_coll",
      vectorCount: 500,
      status: models.CollectionStatus.YELLOW,
      dimensions: 64,
      distanceMetric: models.DistanceMetric.EUCLIDEAN_L2
    };
    expect(desc.name).toBe("desc_coll");
    expect(desc.dimensions).toBe(64);
    expect(desc.distanceMetric).toBe(models.DistanceMetric.EUCLIDEAN_L2);
  });
});
