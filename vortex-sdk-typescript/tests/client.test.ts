/**
 * Unit tests for the synchronous VortexClient.
 */
import * as grpc from '@grpc/grpc-js';
import { VortexClient, VortexClientOptions } from '../src/client';
import * as models from '../src/models';
import * as collections_service_pb from '../src/_grpc/vortex/api/v1/collections_service_pb';
import * as points_service_pb from '../src/_grpc/vortex/api/v1/points_service_pb';
import * as common_pb from '../src/_grpc/vortex/api/v1/common_pb';
import { CollectionsServiceClient } from '../src/_grpc/vortex/api/v1/collections_service_grpc_pb';
import { PointsServiceClient } from '../src/_grpc/vortex/api/v1/points_service_grpc_pb';
import { VortexApiError } from '../src/exceptions';

// Mock the gRPC clients
jest.mock('../src/_grpc/vortex/api/v1/collections_service_grpc_pb');
jest.mock('../src/_grpc/vortex/api/v1/points_service_grpc_pb');

const MockCollectionsServiceClient = CollectionsServiceClient as jest.MockedClass<typeof CollectionsServiceClient>;
const MockPointsServiceClient = PointsServiceClient as jest.MockedClass<typeof PointsServiceClient>;

describe('VortexClient - Collections', () => {
  let client: VortexClient;
  let mockCollectionsStub: jest.Mocked<CollectionsServiceClient>;

  beforeEach(() => {
    MockCollectionsServiceClient.mockClear();
    mockCollectionsStub = new MockCollectionsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<CollectionsServiceClient>;
    (MockCollectionsServiceClient as any).mockImplementation(() => mockCollectionsStub);
    
    const mockPointsStubInstance = new MockPointsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<PointsServiceClient>;
    (MockPointsServiceClient as any).mockImplementation(() => mockPointsStubInstance);

    client = new VortexClient({ host: 'localhost', port: 50051 });
  });

  test('createCollection success', (done) => {
    mockCollectionsStub.createCollection = jest.fn((request, metadata, callbackOrOptions, callback?) => {
      const cb = callback || callbackOrOptions;
      cb(null, new collections_service_pb.CreateCollectionResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;

    const hnswConfig: models.HnswConfigParams = { m: 16, efConstruction: 100, efSearch: 50, ml: 0.5, vectorDim: 128, mMax0: 32 };
    client.createCollection('test-coll', 128, models.DistanceMetric.COSINE, hnswConfig, (err, response) => {
      expect(err).toBeNull();
      expect(response).toBeInstanceOf(collections_service_pb.CreateCollectionResponse);
      expect(mockCollectionsStub.createCollection).toHaveBeenCalledTimes(1);
      const calledRequest = (mockCollectionsStub.createCollection as jest.Mock).mock.calls[0][0] as collections_service_pb.CreateCollectionRequest;
      expect(calledRequest.getCollectionName()).toBe('test-coll');
      expect(calledRequest.getVectorDimensions()).toBe(128);
      done();
    });
  });

  test('getCollectionInfo success', (done) => {
    const grpcResponse = new collections_service_pb.GetCollectionInfoResponse();
    grpcResponse.setCollectionName('test-info');
    grpcResponse.setStatus(collections_service_pb.CollectionStatus.GREEN);
    grpcResponse.setVectorCount(1000);
    const config = new common_pb.HnswConfigParams();
    config.setM(16); config.setEfConstruction(200); config.setEfSearch(100); config.setMl(0.5); config.setVectorDim(128); config.setMMax0(32);
    grpcResponse.setConfig(config);
    grpcResponse.setDistanceMetric(common_pb.DistanceMetric.COSINE);

    mockCollectionsStub.getCollectionInfo = jest.fn((request, metadata, callbackOrOptions, callback?) => {
      const cb = callback || callbackOrOptions;
      cb(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;

    client.getCollectionInfo('test-info', (err, responseModel) => {
      expect(err).toBeNull();
      expect(responseModel).toBeDefined();
      expect(responseModel?.collectionName).toBe('test-info');
      expect(responseModel?.status).toBe(models.CollectionStatus.GREEN);
      expect(responseModel?.vectorCount).toBe(1000);
      done();
    });
  });
  
  test('createCollection gRPC error', (done) => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.INTERNAL,
      details: 'Internal server error',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Internal server error'
    };
    mockCollectionsStub.createCollection = jest.fn((request, metadata, callbackOrOptions, callback?) => {
        const cb = callback || callbackOrOptions;
        cb(grpcError, null);
        return {} as grpc.ClientUnaryCall;
    }) as any;
    client = new VortexClient({ retriesEnabled: false });
    client.createCollection('error-coll', 128, models.DistanceMetric.COSINE, null, (err, response) => {
        expect(err).toBeDefined();
        expect(err!.code).toBe(grpc.status.INTERNAL); // Check code on grpc.ServiceError
        expect(err!.details).toBe('Internal server error');
        expect(response).toBeNull();
        done();
    });
  });

  test('listCollections success', (done) => {
    const grpcResponse = new collections_service_pb.ListCollectionsResponse();
    const desc1 = new collections_service_pb.CollectionDescription();
    desc1.setName('coll1');
    desc1.setVectorCount(100);
    desc1.setStatus(collections_service_pb.CollectionStatus.GREEN);
    desc1.setDimensions(128);
    desc1.setDistanceMetric(common_pb.DistanceMetric.COSINE);
    grpcResponse.addCollections(desc1);

    mockCollectionsStub.listCollections = jest.fn((request, metadata, callbackOrOptions, callback?) => {
      const cb = callback || callbackOrOptions;
      cb(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;

    client.listCollections((err, responseModels) => {
      expect(err).toBeNull();
      expect(responseModels).toBeDefined();
      expect(responseModels?.length).toBe(1);
      expect(responseModels?.[0].name).toBe('coll1');
      expect(responseModels?.[0].vectorCount).toBe(100);
      expect(mockCollectionsStub.listCollections).toHaveBeenCalledTimes(1);
      done();
    });
  });

  test('deleteCollection success', (done) => {
    mockCollectionsStub.deleteCollection = jest.fn((request, metadata, callbackOrOptions, callback?) => {
      const cb = callback || callbackOrOptions;
      cb(null, new collections_service_pb.DeleteCollectionResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;

    client.deleteCollection('test-delete-coll', (err, response) => {
      expect(err).toBeNull();
      expect(response).toBeInstanceOf(collections_service_pb.DeleteCollectionResponse);
      expect(mockCollectionsStub.deleteCollection).toHaveBeenCalledTimes(1);
      const calledRequest = (mockCollectionsStub.deleteCollection as jest.Mock).mock.calls[0][0] as collections_service_pb.DeleteCollectionRequest;
      expect(calledRequest.getCollectionName()).toBe('test-delete-coll');
      done();
    });
  });

  test('deleteCollection gRPC error', (done) => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.NOT_FOUND,
      details: 'Collection not found',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Collection not found'
    };
    mockCollectionsStub.deleteCollection = jest.fn((request, metadata, callbackOrOptions, callback?) => {
      const cb = callback || callbackOrOptions;
      cb(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    client = new VortexClient({ retriesEnabled: false });
    client.deleteCollection('non-existent-coll', (err, response) => {
      expect(err).toBeDefined();
      expect(err!.code).toBe(grpc.status.NOT_FOUND); // Check code on grpc.ServiceError
      expect(response).toBeNull();
      done();
    });
  });

  test('createCollection with timeout', (done) => {
    client = new VortexClient({ host: 'localhost', port: 50051, timeout: 5000 });
    mockCollectionsStub.createCollection = jest.fn((request, metadata, options, callback) => {
      expect(options).toBeDefined();
      expect(options!.deadline).toBeInstanceOf(Date);
      const expectedDeadline = Date.now() + 5000;
      expect((options!.deadline as Date).getTime()).toBeGreaterThanOrEqual(expectedDeadline - 100);
      expect((options!.deadline as Date).getTime()).toBeLessThanOrEqual(expectedDeadline + 100);
      callback(null, new collections_service_pb.CreateCollectionResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
    client.createCollection('timeout-coll', 128, models.DistanceMetric.EUCLIDEAN_L2, null, (err, response) => {
      expect(err).toBeNull();
      expect(response).toBeInstanceOf(collections_service_pb.CreateCollectionResponse);
      done();
    });
  });

  test('createCollectionAsync success', async () => {
    mockCollectionsStub.createCollection = jest.fn((request, metadata, options, callback) => {
      callback(null, new collections_service_pb.CreateCollectionResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const hnswConfig: models.HnswConfigParams = { m: 16, efConstruction: 100, efSearch: 50, ml: 0.5, vectorDim: 128, mMax0: 32 };
    const response = await client.createCollectionAsync('test-async-coll', 128, models.DistanceMetric.COSINE, hnswConfig);
    expect(response).toBeInstanceOf(collections_service_pb.CreateCollectionResponse);
    expect(mockCollectionsStub.createCollection).toHaveBeenCalledTimes(1);
    const calledRequest = (mockCollectionsStub.createCollection as jest.Mock).mock.calls[0][0] as collections_service_pb.CreateCollectionRequest;
    expect(calledRequest.getCollectionName()).toBe('test-async-coll');
  });

  test('createCollectionAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.ALREADY_EXISTS,
      details: 'Collection already exists',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Collection already exists'
    };
    mockCollectionsStub.createCollection = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.createCollectionAsync('test-coll-already-exists', 128, models.DistanceMetric.COSINE);
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.ALREADY_EXISTS);
      expect(err.details).toBe('Collection already exists');
    }
  });

  test('getCollectionInfoAsync success', async () => {
    const grpcResponse = new collections_service_pb.GetCollectionInfoResponse();
    grpcResponse.setCollectionName('test-info-async');
    grpcResponse.setStatus(collections_service_pb.CollectionStatus.GREEN);
    grpcResponse.setVectorCount(1234);
    const config = new common_pb.HnswConfigParams();
    config.setM(24); config.setEfConstruction(150); config.setEfSearch(75); config.setMl(0.6); config.setVectorDim(256); config.setMMax0(48);
    grpcResponse.setConfig(config);
    grpcResponse.setDistanceMetric(common_pb.DistanceMetric.EUCLIDEAN_L2);
    mockCollectionsStub.getCollectionInfo = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const responseModel = await client.getCollectionInfoAsync('test-info-async');
    expect(responseModel).toBeDefined();
    expect(responseModel.collectionName).toBe('test-info-async');
    expect(responseModel.status).toBe(models.CollectionStatus.GREEN);
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });

  test('getCollectionInfoAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.NOT_FOUND,
      details: 'Collection not found async',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Collection not found async'
    };
    mockCollectionsStub.getCollectionInfo = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.getCollectionInfoAsync('non-existent-async');
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.NOT_FOUND);
      expect(err.details).toBe('Collection not found async');
    }
  });

  test('listCollectionsAsync success', async () => {
    const grpcResponse = new collections_service_pb.ListCollectionsResponse();
    const desc1 = new collections_service_pb.CollectionDescription();
    desc1.setName('coll-async-1');
    grpcResponse.addCollections(desc1);
    mockCollectionsStub.listCollections = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const responseModels = await client.listCollectionsAsync();
    expect(responseModels).toBeDefined();
    expect(responseModels.length).toBe(1);
    expect(responseModels[0].name).toBe('coll-async-1');
    expect(mockCollectionsStub.listCollections).toHaveBeenCalledTimes(1);
  });

  test('listCollectionsAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.INTERNAL,
      details: 'Server unavailable for list async',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Server unavailable for list async'
    };
    mockCollectionsStub.listCollections = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.listCollectionsAsync();
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.INTERNAL);
      expect(err.details).toBe('Server unavailable for list async');
    }
  });

  test('deleteCollectionAsync success', async () => {
    mockCollectionsStub.deleteCollection = jest.fn((request, metadata, options, callback) => {
      callback(null, new collections_service_pb.DeleteCollectionResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const response = await client.deleteCollectionAsync('test-delete-async-coll');
    expect(response).toBeInstanceOf(collections_service_pb.DeleteCollectionResponse);
    expect(mockCollectionsStub.deleteCollection).toHaveBeenCalledTimes(1);
    const calledRequest = (mockCollectionsStub.deleteCollection as jest.Mock).mock.calls[0][0] as collections_service_pb.DeleteCollectionRequest;
    expect(calledRequest.getCollectionName()).toBe('test-delete-async-coll');
  });

  test('deleteCollectionAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.PERMISSION_DENIED,
      details: 'Permission denied for delete async',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Permission denied for delete async'
    };
    mockCollectionsStub.deleteCollection = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.deleteCollectionAsync('denied-coll-async');
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.PERMISSION_DENIED);
      expect(err.details).toBe('Permission denied for delete async');
    }
  });
});

describe('VortexClient - Points', () => {
  let client: VortexClient;
  let mockPointsStub: jest.Mocked<PointsServiceClient>;

  beforeEach(() => {
    MockPointsServiceClient.mockClear();
    mockPointsStub = new MockPointsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<PointsServiceClient>;
    (MockPointsServiceClient as any).mockImplementation(() => mockPointsStub);
    const mockCollectionsStubInstance = new MockCollectionsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<CollectionsServiceClient>;
    (MockCollectionsServiceClient as any).mockImplementation(() => mockCollectionsStubInstance);
    client = new VortexClient({ host: 'localhost', port: 50051 });
  });

  test('upsertPoints success', (done) => {
    const grpcResponse = new points_service_pb.UpsertPointsResponse();
    const status = new common_pb.PointOperationStatus();
    status.setPointId('p1');
    status.setStatusCode(common_pb.StatusCode.OK);
    grpcResponse.addStatuses(status);
    mockPointsStub.upsertPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const points: models.PointStruct[] = [{ id: 'p1', vector: { elements: [1, 2] }, payload: { fields: { foo: 'bar' } } }];
    client.upsertPoints('test-coll', points, true, (err, responseModels) => {
      expect(err).toBeNull();
      expect(responseModels).toBeDefined();
      expect(responseModels?.length).toBe(1);
      expect(responseModels?.[0].pointId).toBe('p1');
      expect(responseModels?.[0].statusCode).toBe(models.StatusCode.OK);
      expect(mockPointsStub.upsertPoints).toHaveBeenCalledTimes(1);
      const calledRequest = (mockPointsStub.upsertPoints as jest.Mock).mock.calls[0][0] as points_service_pb.UpsertPointsRequest;
      expect(calledRequest.getCollectionName()).toBe('test-coll');
      expect(calledRequest.getPointsList().length).toBe(1);
      expect(calledRequest.getWaitFlush()).toBe(true);
      done();
    });
  });

  test('upsertPoints with overall_error', (done) => {
    const grpcResponse = new points_service_pb.UpsertPointsResponse();
    grpcResponse.setOverallError("Failed to acquire lock");
    mockPointsStub.upsertPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const points: models.PointStruct[] = [{ id: 'p1', vector: { elements: [1, 2] } }];
    client.upsertPoints('test-coll', points, false, (err, responseModels) => {
      expect(err).toBeDefined();
      expect(err!.code).toBe(grpc.status.UNKNOWN);
      expect(err!.message).toContain("Overall error during upsert: Failed to acquire lock");
      expect(responseModels).toBeNull();
      done();
    });
  });

  test('getPoints success', (done) => {
    const grpcResponse = new points_service_pb.GetPointsResponse();
    const point = new common_pb.PointStruct();
    point.setId('p1');
    const vec = new common_pb.Vector();
    vec.setElementsList([0.1, 0.2]);
    point.setVector(vec);
    grpcResponse.addPoints(point);
    mockPointsStub.getPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    client.getPoints('test-coll', ['p1'], true, true, (err, responseModels) => {
      expect(err).toBeNull();
      expect(responseModels).toBeDefined();
      expect(responseModels?.length).toBe(1);
      expect(responseModels?.[0].id).toBe('p1');
      expect(mockPointsStub.getPoints).toHaveBeenCalledTimes(1);
      const calledRequest = (mockPointsStub.getPoints as jest.Mock).mock.calls[0][0] as points_service_pb.GetPointsRequest;
      expect(calledRequest.getWithPayload()).toBe(true);
      expect(calledRequest.getWithVector()).toBe(true);
      done();
    });
  });
  
  test('deletePoints success', (done) => {
    const grpcResponse = new points_service_pb.DeletePointsResponse();
    const status = new common_pb.PointOperationStatus();
    status.setPointId('p-del');
    status.setStatusCode(common_pb.StatusCode.OK);
    grpcResponse.addStatuses(status);
    mockPointsStub.deletePoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    client.deletePoints('test-coll', ['p-del'], true, (err, responseModels) => {
      expect(err).toBeNull();
      expect(responseModels).toBeDefined();
      expect(responseModels?.[0].statusCode).toBe(models.StatusCode.OK);
      expect(mockPointsStub.deletePoints).toHaveBeenCalledTimes(1);
      done();
    });
  });

  test('searchPoints success', (done) => {
    const grpcResponse = new points_service_pb.SearchPointsResponse();
    const scoredPoint = new common_pb.ScoredPoint();
    scoredPoint.setId('sp1');
    scoredPoint.setScore(0.99);
    const pointPayload = new common_pb.Payload();
    scoredPoint.setPayload(pointPayload);
    grpcResponse.addResults(scoredPoint);
    mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const queryVector: models.Vector = { elements: [0.5, 0.5] };
    const filter: models.Filter = { mustMatchExact: { genre: 'sci-fi' } };
    client.searchPoints('test-coll', queryVector, 10, filter, true, false, undefined, (err, responseModels) => {
      expect(err).toBeNull();
      expect(responseModels).toBeDefined();
      expect(responseModels?.length).toBe(1);
      expect(responseModels?.[0].id).toBe('sp1');
      expect(mockPointsStub.searchPoints).toHaveBeenCalledTimes(1);
      const calledRequest = (mockPointsStub.searchPoints as jest.Mock).mock.calls[0][0] as points_service_pb.SearchPointsRequest;
      expect(calledRequest.getKLimit()).toBe(10);
      expect(calledRequest.getFilter()).toBeDefined();
      done();
    });
  });

  test('searchPoints gRPC error', (done) => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.UNAVAILABLE,
      details: 'Service unavailable',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Service unavailable'
    };
     mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    client = new VortexClient({ retriesEnabled: false });
    client.searchPoints('test-coll', { elements: [1] }, 5, null, true, true, undefined, (err, response) => {
      expect(err).toBeDefined();
      expect(err!.code).toBe(grpc.status.UNAVAILABLE); // Check code on grpc.ServiceError
      expect(response).toBeNull();
      done();
    });
  });

  test('upsertPointsAsync success', async () => {
    const grpcResponse = new points_service_pb.UpsertPointsResponse();
    const status = new common_pb.PointOperationStatus();
    status.setPointId('p-async-1');
    status.setStatusCode(common_pb.StatusCode.OK);
    grpcResponse.addStatuses(status);
    mockPointsStub.upsertPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const points: models.PointStruct[] = [{ id: 'p-async-1', vector: { elements: [3, 4] } }];
    const responseModels = await client.upsertPointsAsync('test-coll-async', points, true);
    expect(responseModels).toBeDefined();
    expect(responseModels.length).toBe(1);
    expect(responseModels[0].pointId).toBe('p-async-1');
    expect(responseModels[0].statusCode).toBe(models.StatusCode.OK);
    expect(mockPointsStub.upsertPoints).toHaveBeenCalledTimes(1);
  });

  test('upsertPointsAsync with overall_error', async () => {
    const grpcResponse = new points_service_pb.UpsertPointsResponse();
    grpcResponse.setOverallError("Async upsert failed");
    mockPointsStub.upsertPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const points: models.PointStruct[] = [{ id: 'p-async-err', vector: { elements: [5, 6] } }];
    try {
      await client.upsertPointsAsync('test-coll-async-err', points, false);
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.message).toContain("Async upsert failed");
      expect(err.statusCode).toBe(grpc.status.UNKNOWN);
    }
  });

  test('getPointsAsync success', async () => {
    const grpcResponse = new points_service_pb.GetPointsResponse();
    const point = new common_pb.PointStruct();
    point.setId('p-async-get');
    grpcResponse.addPoints(point);
    mockPointsStub.getPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const responseModels = await client.getPointsAsync('test-coll-async', ['p-async-get'], true, false);
    expect(responseModels).toBeDefined();
    expect(responseModels.length).toBe(1);
    expect(responseModels[0].id).toBe('p-async-get');
    expect(mockPointsStub.getPoints).toHaveBeenCalledTimes(1);
  });

  test('getPointsAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.INTERNAL,
      details: 'Internal error on getPointsAsync',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Internal error on getPointsAsync'
    };
    mockPointsStub.getPoints = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.getPointsAsync('test-coll-async-err', ['id1'], true, true);
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.INTERNAL);
    }
  });

  test('deletePointsAsync success', async () => {
    const grpcResponse = new points_service_pb.DeletePointsResponse();
    const status = new common_pb.PointOperationStatus();
    status.setPointId('p-async-del');
    status.setStatusCode(common_pb.StatusCode.OK);
    grpcResponse.addStatuses(status);
    mockPointsStub.deletePoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const responseModels = await client.deletePointsAsync('test-coll-async', ['p-async-del'], true);
    expect(responseModels).toBeDefined();
    expect(responseModels.length).toBe(1);
    expect(responseModels[0].statusCode).toBe(models.StatusCode.OK);
    expect(mockPointsStub.deletePoints).toHaveBeenCalledTimes(1);
  });

  test('deletePointsAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.ABORTED,
      details: 'Delete aborted async',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Delete aborted async'
    };
    mockPointsStub.deletePoints = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.deletePointsAsync('test-coll-async-err', ['id1'], false);
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.ABORTED);
    }
  });

  test('searchPointsAsync success', async () => {
    const grpcResponse = new points_service_pb.SearchPointsResponse();
    const scoredPoint = new common_pb.ScoredPoint();
    scoredPoint.setId('sp-async-1');
    scoredPoint.setScore(0.88);
    grpcResponse.addResults(scoredPoint);
    mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      callback(null, grpcResponse);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const queryVector: models.Vector = { elements: [0.1, 0.9] };
    const responseModels = await client.searchPointsAsync('test-coll-async', queryVector, 5, null, true, true);
    expect(responseModels).toBeDefined();
    expect(responseModels.length).toBe(1);
    expect(responseModels[0].id).toBe('sp-async-1');
    expect(mockPointsStub.searchPoints).toHaveBeenCalledTimes(1);
  });

  test('searchPointsAsync gRPC error', async () => {
    const grpcError: grpc.ServiceError = {
      code: grpc.status.INVALID_ARGUMENT,
      details: 'Invalid k_limit for searchPointsAsync',
      metadata: new grpc.Metadata(),
      name: 'Error',
      message: 'Invalid k_limit for searchPointsAsync'
    };
    mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      callback(grpcError, null);
      return {} as grpc.ClientUnaryCall;
    }) as any;
    try {
      await client.searchPointsAsync('test-coll-async-err', { elements: [0.1] }, 0, null, false, false, null);
    } catch (err: any) {
      expect(err).toBeInstanceOf(VortexApiError);
      expect(err.statusCode).toBe(grpc.status.INVALID_ARGUMENT);
    }
  });

  test('searchPoints with SearchParams', (done) => {
    mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      const grpcRequest = request as points_service_pb.SearchPointsRequest;
      expect(grpcRequest.getParams()).toBeDefined();
      expect(grpcRequest.getParams()?.getEfSearch()).toBe(150);
      callback(null, new points_service_pb.SearchPointsResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const searchParams: models.SearchParams = { hnsw_ef: 150 };
    client.searchPoints('test-coll', { elements: [1] }, 5, null, true, true, searchParams, (err, _response) => {
      expect(err).toBeNull();
      done();
    });
  });

  test('searchPointsAsync with SearchParams', async () => {
    mockPointsStub.searchPoints = jest.fn((request, metadata, options, callback) => {
      const grpcRequest = request as points_service_pb.SearchPointsRequest;
      expect(grpcRequest.getParams()).toBeDefined();
      expect(grpcRequest.getParams()?.getEfSearch()).toBe(200);
      callback(null, new points_service_pb.SearchPointsResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
    const searchParams: models.SearchParams = { hnsw_ef: 200 };
    await client.searchPointsAsync('test-coll', { elements: [1] }, 5, null, true, true, searchParams);
    expect(mockPointsStub.searchPoints).toHaveBeenCalledTimes(1);
  });
});

describe('VortexClient - General', () => {
  let client: VortexClient;
  let mockCollectionsStub: jest.Mocked<CollectionsServiceClient>;
  let mockPointsStub: jest.Mocked<PointsServiceClient>;

  beforeEach(() => {
    MockCollectionsServiceClient.mockClear();
    MockPointsServiceClient.mockClear();
    mockCollectionsStub = new MockCollectionsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<CollectionsServiceClient>;
    mockPointsStub = new MockPointsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<PointsServiceClient>;
    (MockCollectionsServiceClient as any).mockImplementation(() => mockCollectionsStub);
    (MockPointsServiceClient as any).mockImplementation(() => mockPointsStub);
    client = new VortexClient({ host: 'localhost', port: 50051 });
  });

  test('client.close() calls close on stubs', () => {
    expect((client as any)._collectionsStub).toBeInstanceOf(MockCollectionsServiceClient);
    expect((client as any)._pointsStub).toBeInstanceOf(MockPointsServiceClient);
    const collectionCloseSpy = jest.spyOn(mockCollectionsStub, 'close');
    const pointsCloseSpy = jest.spyOn(mockPointsStub, 'close');
    client.close();
    expect(collectionCloseSpy).toHaveBeenCalledTimes(1);
    expect(pointsCloseSpy).toHaveBeenCalledTimes(1);
    expect((client as any)._collectionsStub).toBeNull();
    expect((client as any)._pointsStub).toBeNull();
    // expect((client as any)._channel).toBeNull(); // _channel is not actively used or nulled in client.ts
  });

  test('client methods call callback with error if stubs are null (simulating not connected)', (done) => {
    (client as any)._collectionsStub = null;
    (client as any)._pointsStub = null;
    client.getCollectionInfo('any-coll', (err, response) => {
      expect(err).toBeDefined();
      expect(err!.code).toBe(grpc.status.UNAVAILABLE); // grpc.ServiceError has 'code'
      expect(err!.message).toBe('Client not connected');
      expect(response).toBeNull();
      
      client.getPoints('any-coll', ['id1'], false, false, (errPoints, responsePoints) => {
        expect(errPoints).toBeDefined();
        expect(errPoints!.code).toBe(grpc.status.UNAVAILABLE); // grpc.ServiceError has 'code'
        expect(errPoints!.message).toBe('Client not connected');
        expect(responsePoints).toBeNull();
        done();
      });
    });
  });

  test('client instantiates with default host and port if not provided', () => {
    const defaultClient = new VortexClient();
    expect((defaultClient as any).clientOptions.host).toBe('localhost');
    expect((defaultClient as any).clientOptions.port).toBe(50051);
  });

  test('client instantiates with provided apiKey and timeout', () => {
    const options = { apiKey: 'test-key', timeout: 10000 };
    const configuredClient = new VortexClient(options);
    expect((configuredClient as any).clientOptions.apiKey).toBe('test-key');
    expect((configuredClient as any).clientOptions.timeout).toBe(10000);
  });

  describe('constructor URL parsing and validation', () => {
    test('should parse URL correctly for http', () => {
      const client = new VortexClient({ url: 'http://myvortex.com:1234/path/prefix' });
      const opts = (client as any).clientOptions;
      expect(opts.host).toBe('myvortex.com');
      expect(opts.port).toBe(1234);
      expect(opts.secure).toBe(false);
      expect(opts.prefix).toBe('/path/prefix');
    });

    test('should parse URL correctly for https with default port', () => {
      const client = new VortexClient({ url: 'https://secure-vortex.dev' });
      const opts = (client as any).clientOptions;
      expect(opts.host).toBe('secure-vortex.dev');
      expect(opts.port).toBe(443);
      expect(opts.secure).toBe(true);
      expect(opts.prefix).toBe('');
    });
    
    test('should parse URL with prefix and remove trailing slash', () => {
      const client = new VortexClient({ url: 'http://localhost:8080/api/v1/' });
      const opts = (client as any).clientOptions;
      expect(opts.host).toBe('localhost');
      expect(opts.port).toBe(8080);
      expect(opts.secure).toBe(false);
      expect(opts.prefix).toBe('/api/v1');
    });

    test('should use default port 80 for http if not specified in URL', () => {
      const client = new VortexClient({ url: 'http://no-port-vortex.com' });
      const opts = (client as any).clientOptions;
      expect(opts.port).toBe(80);
      expect(opts.secure).toBe(false);
    });

    test('should throw error if both url and host/port are provided', () => {
      expect(() => new VortexClient({ url: 'http://localhost', host: 'localhost' })).toThrow(VortexApiError);
      expect(() => new VortexClient({ url: 'http://localhost', port: 1234 })).toThrow(VortexApiError);
    });

    test('should throw error if host contains protocol', () => {
      expect(() => new VortexClient({ host: 'http://localhost' })).toThrow(VortexApiError);
    });

    test('should throw error if host contains port', () => {
      expect(() => new VortexClient({ host: 'localhost:1234' })).toThrow(VortexApiError);
    });
    
    test('should throw error if URL does not start with http:// or https://', () => {
      expect(() => new VortexClient({ url: 'myvortex.com' })).toThrow(VortexApiError);
    });

    test('should correctly parse valid IPv6 URL', () => {
      expect(() => new VortexClient({ url: 'http://[::1]:8080' })).not.toThrow();
      const client = new VortexClient({ url: 'http://[::1]:8080' });
      const opts = (client as any).clientOptions;
      expect(opts.host).toBe('[::1]');
      expect(opts.port).toBe(8080);
      expect(opts.secure).toBe(false);
    });
    
    test('should use default host and port if only secure=true is provided', () => {
        const client = new VortexClient({ secure: true });
        const opts = (client as any).clientOptions;
        expect(opts.host).toBe('localhost');
        expect(opts.port).toBe(443); // Default secure port
        expect(opts.secure).toBe(true);
    });

    test('should use default host and port 50051 if only secure=false is provided', () => {
        const client = new VortexClient({ secure: false });
        const opts = (client as any).clientOptions;
        expect(opts.host).toBe('localhost');
        expect(opts.port).toBe(50051); // Default insecure gRPC port
        expect(opts.secure).toBe(false);
    });
    
    test('should warn if apiKey is used with insecure connection via URL', () => {
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
      new VortexClient({ url: 'http://myvortex.com', apiKey: 'test-key' });
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('API key is used with an insecure connection'));
      consoleWarnSpy.mockRestore();
    });

    test('should warn if apiKey is used with insecure connection via secure=false', () => {
      const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
      new VortexClient({ host: 'myvortex.com', secure: false, apiKey: 'test-key' });
      expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('API key is used with an insecure connection'));
      consoleWarnSpy.mockRestore();
    });
  });

  test('constructor sets default retry options if not provided', () => {
    const client = new VortexClient();
    const opts = (client as any).clientOptions;
    expect(opts.retriesEnabled).toBe(true);
    expect(opts.maxRetries).toBe(3);
    expect(opts.initialBackoffMs).toBe(200);
    expect(opts.maxBackoffMs).toBe(5000);
    expect(opts.backoffMultiplier).toBe(1.5);
    expect(opts.retryJitter).toBe(0.1);
    expect(opts.retryableStatusCodes).toEqual([grpc.status.UNAVAILABLE, grpc.status.RESOURCE_EXHAUSTED]);
  });

  test('constructor uses provided retry options', () => {
    const retryOpts = {
      retriesEnabled: false,
      maxRetries: 5,
      initialBackoffMs: 100,
      maxBackoffMs: 10000,
      backoffMultiplier: 2,
      retryJitter: 0.05,
      retryableStatusCodes: [grpc.status.UNAVAILABLE],
    };
    const client = new VortexClient(retryOpts);
    const opts = (client as any).clientOptions;
    expect(opts.retriesEnabled).toBe(retryOpts.retriesEnabled);
    expect(opts.maxRetries).toBe(retryOpts.maxRetries);
    expect(opts.initialBackoffMs).toBe(retryOpts.initialBackoffMs);
    expect(opts.maxBackoffMs).toBe(retryOpts.maxBackoffMs);
    expect(opts.backoffMultiplier).toBe(retryOpts.backoffMultiplier);
    expect(opts.retryJitter).toBe(retryOpts.retryJitter);
    expect(opts.retryableStatusCodes).toEqual(retryOpts.retryableStatusCodes);
  });
});

describe('VortexClient - Connection Options', () => {
  let mockCreateInsecure: jest.SpyInstance;
  let mockCreateSsl: jest.SpyInstance;

  beforeEach(() => {
    mockCreateInsecure = jest.spyOn(grpc.credentials, 'createInsecure');
    mockCreateSsl = jest.spyOn(grpc.credentials, 'createSsl');
    MockCollectionsServiceClient.mockClear();
    MockPointsServiceClient.mockClear();
  });

  afterEach(() => {
    mockCreateInsecure.mockRestore();
    mockCreateSsl.mockRestore();
  });

  test('uses insecure credentials by default', () => {
    new VortexClient();
    expect(mockCreateInsecure).toHaveBeenCalledTimes(1);
    expect(mockCreateSsl).not.toHaveBeenCalled();
    expect(MockCollectionsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, undefined);
    expect(MockPointsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, undefined);
  });

  test('uses insecure credentials when secure is false', () => {
    new VortexClient({ secure: false });
    expect(mockCreateInsecure).toHaveBeenCalledTimes(1);
    expect(mockCreateSsl).not.toHaveBeenCalled();
    expect(MockCollectionsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, undefined);
    expect(MockPointsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, undefined);
  });

  test('uses SSL credentials when secure is true', () => {
    const rootCerts = Buffer.from("fakeRootCertContent");
    const privateKey = Buffer.from("fakePrivateKeyContent");
    const certChain = Buffer.from("fakeCertChainContent");
    const dummySslCredentials = {} as grpc.ChannelCredentials;
    mockCreateSsl.mockReturnValue(dummySslCredentials);
    new VortexClient({ secure: true, rootCerts, privateKey, certChain });
    expect(mockCreateSsl).toHaveBeenCalledTimes(1);
    expect(mockCreateSsl).toHaveBeenCalledWith(rootCerts, privateKey, certChain);
    expect(mockCreateInsecure).not.toHaveBeenCalled();
    expect(MockCollectionsServiceClient).toHaveBeenCalledWith(expect.any(String), dummySslCredentials, undefined);
    expect(MockPointsServiceClient).toHaveBeenCalledWith(expect.any(String), dummySslCredentials, undefined);
  });

  test('passes grpcClientOptions to service client constructors', () => {
    const grpcOptions: grpc.ChannelOptions = { 'grpc.keepalive_time_ms': 10000 };
    new VortexClient({ grpcClientOptions: grpcOptions });
    expect(mockCreateInsecure).toHaveBeenCalledTimes(1);
    expect(MockCollectionsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, grpcOptions);
    expect(MockPointsServiceClient).toHaveBeenCalledWith(expect.any(String), mockCreateInsecure.mock.results[0].value, grpcOptions);
  });

   test('passes grpcClientOptions with SSL credentials', () => {
    const grpcOptions: grpc.ChannelOptions = { 'grpc.max_receive_message_length': -1 };
    const rootCerts = Buffer.from('testRoot');
    const dummySslCredentials = {} as grpc.ChannelCredentials;
    mockCreateSsl.mockReturnValue(dummySslCredentials);
    new VortexClient({ secure: true, rootCerts, grpcClientOptions: grpcOptions });
    expect(mockCreateSsl).toHaveBeenCalledTimes(1);
    expect(MockCollectionsServiceClient).toHaveBeenCalledWith(expect.any(String), dummySslCredentials, grpcOptions);
    expect(MockPointsServiceClient).toHaveBeenCalledWith(expect.any(String), dummySslCredentials, grpcOptions);
  });
});

describe('VortexClient - Retry Logic', () => {
  let client: VortexClient;
  let mockCollectionsStub: jest.Mocked<CollectionsServiceClient>;
  jest.useFakeTimers();

  const createMockGrpcError = (code: grpc.status, details: string): grpc.ServiceError => ({
    code, details, metadata: new grpc.Metadata(), name: 'Error', message: details,
  });

  beforeEach(() => {
    MockCollectionsServiceClient.mockClear();
    mockCollectionsStub = new MockCollectionsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<CollectionsServiceClient>;
    mockCollectionsStub.getCollectionInfo = jest.fn();
    (MockCollectionsServiceClient as any).mockImplementation(() => mockCollectionsStub);
    const mockPointsStubInstance = new MockPointsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<PointsServiceClient>;
    (MockPointsServiceClient as any).mockImplementation(() => mockPointsStubInstance);
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  test('should not retry if retriesEnabled is false', async () => {
    client = new VortexClient({ retriesEnabled: false });
    const error = createMockGrpcError(grpc.status.UNAVAILABLE, 'Service unavailable');
    mockCollectionsStub.getCollectionInfo.mockImplementationOnce((req, meta, opts, cb) => {
      cb(error, null); return {} as grpc.ClientUnaryCall;
    });
    try {
      await client.getCollectionInfoAsync('test-coll');
    } catch (e: any) {
      expect(e).toBeInstanceOf(VortexApiError);
      expect(e.statusCode).toBe(grpc.status.UNAVAILABLE);
      expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
    }
  });

  test('should succeed on first attempt if no error', async () => {
    client = new VortexClient({ retriesEnabled: true });
    const grpcResponse = new collections_service_pb.GetCollectionInfoResponse();
    grpcResponse.setCollectionName('test-coll');
    const hnswConfigPbSuccess = new common_pb.HnswConfigParams();
    hnswConfigPbSuccess.setM(16); hnswConfigPbSuccess.setEfConstruction(200); hnswConfigPbSuccess.setEfSearch(100);
    hnswConfigPbSuccess.setMl(0.5); hnswConfigPbSuccess.setVectorDim(128); hnswConfigPbSuccess.setMMax0(32);
    grpcResponse.setConfig(hnswConfigPbSuccess);
    grpcResponse.setDistanceMetric(common_pb.DistanceMetric.COSINE);
    mockCollectionsStub.getCollectionInfo.mockImplementationOnce((req, meta, opts, cb) => {
      cb(null, grpcResponse); return {} as grpc.ClientUnaryCall;
    });
    const response = await client.getCollectionInfoAsync('test-coll');
    expect(response.collectionName).toBe('test-coll');
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });

  test('should retry on retryable error and succeed', async () => {
    client = new VortexClient({ initialBackoffMs: 10, retriesEnabled: true });
    const error = createMockGrpcError(grpc.status.UNAVAILABLE, 'Service temporarily unavailable');
    const successResponse = new collections_service_pb.GetCollectionInfoResponse();
    successResponse.setCollectionName('retry-success-coll');
    const hnswConfigPbRetry = new common_pb.HnswConfigParams();
    hnswConfigPbRetry.setM(16); hnswConfigPbRetry.setEfConstruction(200); hnswConfigPbRetry.setEfSearch(100);
    hnswConfigPbRetry.setMl(0.5); hnswConfigPbRetry.setVectorDim(128); hnswConfigPbRetry.setMMax0(32);
    successResponse.setConfig(hnswConfigPbRetry);
    successResponse.setDistanceMetric(common_pb.DistanceMetric.COSINE);
    mockCollectionsStub.getCollectionInfo
      .mockImplementationOnce((req, meta, opts, cb) => { cb(error, null); return {} as grpc.ClientUnaryCall; })
      .mockImplementationOnce((req, meta, opts, cb) => { cb(null, successResponse); return {} as grpc.ClientUnaryCall; });
    const promise = client.getCollectionInfoAsync('retry-success-coll');

    // Allow first call and its promise to settle (schedules first setTimeout)
    await Promise.resolve();
    await jest.advanceTimersByTimeAsync(0);
    await Promise.resolve();

    // First retry
    await jest.advanceTimersByTimeAsync(10); // Advance by initialBackoffMs
    await Promise.resolve(); // Allow setTimeout promise to resolve, next attempt (success) starts
    await Promise.resolve(); // Extra flush

    // Successful attempt should have completed.
    const response = await promise;
    expect(response.collectionName).toBe('retry-success-coll');
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(2);
  });

  test('should exhaust retries and fail for persistent retryable error', async () => {
    client = new VortexClient({ maxRetries: 2, initialBackoffMs: 10 });
    const error = createMockGrpcError(grpc.status.RESOURCE_EXHAUSTED, 'Rate limited');
    mockCollectionsStub.getCollectionInfo.mockImplementation((req, meta, opts, cb) => {
      cb(error, null); return {} as grpc.ClientUnaryCall;
    });
    
    let caughtError: any = null;
    const promise = client.getCollectionInfoAsync('exhaust-retries-coll').catch(e => { caughtError = e; });
    
    // Initial call + 2 retries. Backoffs: 10ms, then 10 * 1.5 = 15ms. Total timer time = 25ms.
    await jest.advanceTimersByTimeAsync(25 + 1); 
    jest.runAllTicks();
    await Promise.resolve();
    jest.runAllTicks();
    await Promise.resolve();

    // Ensure the promise itself has settled if it was going to.
    // It might have already been caught by the .catch() handler.
    // If not, awaiting it here will throw, and it will be caught by the outer try/catch.
    try {
      await promise; 
    } catch (e) {
      if (!caughtError) caughtError = e; // If .catch() didn't run, this will catch it.
    }
    
    expect(caughtError).toBeInstanceOf(VortexApiError);
    if (caughtError) { 
      expect(caughtError.message).toContain('Failed to getCollectionInfoAsync after 3 attempts');
      expect(caughtError.statusCode).toBe(grpc.status.RESOURCE_EXHAUSTED);
      expect(caughtError.details).toBe('Rate limited');
    }
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(3);
  });

  test('should not retry on non-retryable error', async () => {
    client = new VortexClient({ retriesEnabled: true, initialBackoffMs: 10 });
    const error = createMockGrpcError(grpc.status.INVALID_ARGUMENT, 'Invalid collection name');
    mockCollectionsStub.getCollectionInfo.mockImplementationOnce((req, meta, opts, cb) => {
      cb(error, null); return {} as grpc.ClientUnaryCall;
    });
    try {
      await client.getCollectionInfoAsync('invalid-coll-name');
    } catch (e: any) {
      expect(e).toBeInstanceOf(VortexApiError);
      expect(e.statusCode).toBe(grpc.status.INVALID_ARGUMENT);
      expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
    }
  });

  test('should attempt once and not retry if maxRetries is 0, even if retriesEnabled is true', async () => {
    client = new VortexClient({ retriesEnabled: true, maxRetries: 0 });
    const error = createMockGrpcError(grpc.status.UNAVAILABLE, 'Service unavailable, maxRetries: 0');
    mockCollectionsStub.getCollectionInfo.mockImplementationOnce((req, meta, opts, cb) => {
      cb(error, null); return {} as grpc.ClientUnaryCall;
    });
    try {
      await client.getCollectionInfoAsync('test-coll-max-retries-0');
    } catch (e: any) {
      expect(e).toBeInstanceOf(VortexApiError);
      expect(e.statusCode).toBe(grpc.status.UNAVAILABLE);
      expect(e.message).toContain('Failed to getCollectionInfoAsync attempt 1'); // Attempt 1 (initial) fails
      expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1); // Called only once
    }
  });
  
  test('callback-style method should also retry and succeed - revised', async () => {
    client = new VortexClient({ initialBackoffMs: 10, retriesEnabled: true, maxRetries: 1 });
    const error = createMockGrpcError(grpc.status.UNAVAILABLE, 'Callback unavailable');
    const successResponse = new collections_service_pb.GetCollectionInfoResponse();
    successResponse.setCollectionName('cb-retry-success-revised');
    const hnswConfigPbCb = new common_pb.HnswConfigParams();
    hnswConfigPbCb.setM(16); hnswConfigPbCb.setEfConstruction(200); hnswConfigPbCb.setEfSearch(100);
    hnswConfigPbCb.setMl(0.5); hnswConfigPbCb.setVectorDim(128); hnswConfigPbCb.setMMax0(32);
    successResponse.setConfig(hnswConfigPbCb);
    successResponse.setDistanceMetric(common_pb.DistanceMetric.COSINE);

    mockCollectionsStub.getCollectionInfo
      .mockImplementationOnce((req, meta, opts, cb) => { cb(error, null); return {} as grpc.ClientUnaryCall; })
      .mockImplementationOnce((req, meta, opts, cb) => { cb(null, successResponse); return {} as grpc.ClientUnaryCall; });

    const testPromise = new Promise<void>((resolve, reject) => {
      client.getCollectionInfo('cb-retry-success-revised', (err, responseModel) => {
        try {
          expect(err).toBeNull();
          expect(responseModel?.collectionName).toBe('cb-retry-success-revised');
          expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(2);
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    });

    // Allow the first call to happen and its promise to settle
    await Promise.resolve(); 
    await jest.advanceTimersByTimeAsync(0); // Ensure any immediate timers run
    await Promise.resolve();

    // Advance timer for the first backoff (initialBackoffMs = 10ms)
    await jest.advanceTimersByTimeAsync(10); // For the first retry
    await Promise.resolve(); 
    await Promise.resolve(); // Extra flush

    await testPromise; 
  }, 10000);

  test('callback-style method should exhaust retries and call callback with error - revised', async () => {
    client = new VortexClient({ maxRetries: 1, initialBackoffMs: 10, retriesEnabled: true });
    const error = createMockGrpcError(grpc.status.UNAVAILABLE, 'CB persistent unavailable revised');
    
    mockCollectionsStub.getCollectionInfo.mockImplementation((req, meta, opts, cb) => {
      cb(error, null); return {} as grpc.ClientUnaryCall;
    });

    const testPromise = new Promise<void>((resolve, reject) => {
      client.getCollectionInfo('cb-exhaust-retries-revised', (err, responseModel) => {
        try {
          expect(err).toBeDefined();
          // The finalCallback in _execute_with_retry_ts is called with the grpc.ServiceError directly
          expect(err!.code).toBe(grpc.status.UNAVAILABLE);
          expect(err!.message).toBe('CB persistent unavailable revised'); // Check message on grpc.ServiceError
          expect(responseModel).toBeNull();
          // maxRetries = 1 means 1 initial attempt + 1 retry = 2 calls
          expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(2);
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    });

    // Allow the first call to happen and its promise to settle
    await Promise.resolve(); // Flush microtasks from initial call setup
    await jest.advanceTimersByTimeAsync(0); // Ensure any immediate timers run (e.g. if backoff was 0)
    await Promise.resolve();

    // Advance timer for the first backoff (initialBackoffMs = 10ms)
    // The client will make the first call, it will fail, then schedule a setTimeout.
    await jest.advanceTimersByTimeAsync(10);
    await Promise.resolve(); // Allow the setTimeout promise to resolve and the next attempt to start
    await Promise.resolve(); // Extra flush for safety

    // The second call (first retry) will be made. It will also fail.
    // Since maxRetries is 1, this is the last attempt. The finalCallback should be invoked.
    // No more timers should be scheduled.

    await testPromise; // Wait for the callback assertions to complete.
  }, 10000);
});

// Helper function to simulate a hanging gRPC call, defined at a higher scope
const mockHangingCall = (stubMethod: jest.Mock) => {
  stubMethod.mockImplementation((req, meta, opts, cb) => {
    // Do nothing, just hang, will be cancelled by timeout or test
    const call = { cancel: jest.fn() } as unknown as grpc.ClientUnaryCall;
    return call;
  });
};

describe('VortexClient - Client-Side Timeout Logic', () => {
  let client: VortexClient;
  let mockCollectionsStub: jest.Mocked<CollectionsServiceClient>;
  jest.useFakeTimers();

  const createMockGrpcError = (code: grpc.status, details: string): grpc.ServiceError => ({
    code, details, metadata: new grpc.Metadata(), name: 'Error', message: details,
  });

  beforeEach(() => {
    MockCollectionsServiceClient.mockClear();
    mockCollectionsStub = new MockCollectionsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<CollectionsServiceClient>;
    mockCollectionsStub.getCollectionInfo = jest.fn();
    (MockCollectionsServiceClient as any).mockImplementation(() => mockCollectionsStub);
     // Ensure PointsServiceClient is also mocked if client constructor tries to connect to it
    const mockPointsStubInstance = new MockPointsServiceClient('localhost:50051', grpc.credentials.createInsecure()) as jest.Mocked<PointsServiceClient>;
    (MockPointsServiceClient as any).mockImplementation(() => mockPointsStubInstance);
  });

  afterEach(() => {
    jest.clearAllTimers();
  });

  test('should timeout client-side if requestTimeoutMs is exceeded', async () => {
    client = new VortexClient({ requestTimeoutMs: 100, retriesEnabled: false });
    mockHangingCall(mockCollectionsStub.getCollectionInfo as jest.Mock);
    
    let caughtError: any = null;
    const promise = client.getCollectionInfoAsync('timeout-test-coll').catch(e => { caughtError = e; });
    
    await jest.advanceTimersByTimeAsync(100 + 1); // Advance past the timeout
    jest.runAllTicks();
    await Promise.resolve();
    jest.runAllTicks();
    await Promise.resolve();

    try {
      await promise; 
    } catch (e) {
      if (!caughtError) caughtError = e;
    }

    expect(caughtError).toBeInstanceOf(VortexApiError);
    if (caughtError) {
      expect(caughtError.statusCode).toBe(grpc.status.CANCELLED);
      expect(caughtError.message).toContain('Request timed out client-side after 100ms');
      expect(caughtError.isClientTimeout).toBe(true);
    }
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });

  test('metadata includes user-agent and api-key if provided', (done) => {
    const apiKey = 'test-api-key';
    client = new VortexClient({ apiKey, retriesEnabled: false }); // Disable retries for simplicity
  
    mockCollectionsStub.getCollectionInfo = jest.fn((request, metadata, options, callback) => {
      expect(metadata).toBeDefined();
      expect(metadata.get('user-agent')[0]).toBe(`vortex-sdk-ts/0.1.0`);
      expect(metadata.get('api-key')[0]).toBe(apiKey);
      callback(null, new collections_service_pb.GetCollectionInfoResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
  
    client.getCollectionInfo('meta-test-coll', (err, _response) => {
      expect(err).toBeNull();
      expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
      done();
    });
  });

  test('metadata includes only user-agent if api-key is not provided', (done) => {
    client = new VortexClient({ retriesEnabled: false }); // Disable retries
  
    mockCollectionsStub.getCollectionInfo = jest.fn((request, metadata, options, callback) => {
      expect(metadata).toBeDefined();
      expect(metadata.get('user-agent')[0]).toBe(`vortex-sdk-ts/0.1.0`);
      expect(metadata.get('api-key').length).toBe(0); // api-key should not be set
      callback(null, new collections_service_pb.GetCollectionInfoResponse());
      return {} as grpc.ClientUnaryCall;
    }) as any;
  
    client.getCollectionInfo('no-apikey-coll', (err, _response) => {
      expect(err).toBeNull();
      expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
      done();
    });
  });

  test('client-side timeout should not be retried even if CANCELLED is retryable', async () => {
    client = new VortexClient({ 
      requestTimeoutMs: 50, 
      maxRetries: 1, 
      initialBackoffMs: 10,
      retryableStatusCodes: [grpc.status.UNAVAILABLE, grpc.status.CANCELLED] // Add CANCELLED to retryable
    });
    mockHangingCall(mockCollectionsStub.getCollectionInfo as jest.Mock);

    let caughtError: any = null;
    const promise = client.getCollectionInfoAsync('no-retry-timeout-coll').catch(e => { caughtError = e; });
    
    await jest.advanceTimersByTimeAsync(50 + 1); 
    jest.runAllTicks();
    await Promise.resolve();
    jest.runAllTicks();
    await Promise.resolve();

    try {
      await promise;
    } catch (e) {
      if (!caughtError) caughtError = e;
    }
    
    expect(caughtError).toBeInstanceOf(VortexApiError);
    if (caughtError) {
      expect(caughtError.statusCode).toBe(grpc.status.CANCELLED);
      expect(caughtError.message).toContain('Request timed out client-side after 50ms');
      expect(caughtError.isClientTimeout).toBe(true);
    }
    // Should only be called once because client-side timeouts are not retried
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });

  test('gRPC deadline (timeout) should still work if requestTimeoutMs is longer or not set', async () => {
    client = new VortexClient({ timeout: 50, requestTimeoutMs: 200, retriesEnabled: false }); // gRPC deadline is shorter
    
    mockCollectionsStub.getCollectionInfo.mockImplementationOnce((req, meta, opts, cb) => {
      // Simulate gRPC deadline exceeded
      setTimeout(() => { // Ensure this runs after the deadline would have passed
        cb(createMockGrpcError(grpc.status.DEADLINE_EXCEEDED, 'gRPC deadline hit'), null);
      }, 60);
      return { cancel: jest.fn() } as unknown as grpc.ClientUnaryCall;
    });

    let caughtError: any = null;
    const promise = client.getCollectionInfoAsync('grpc-deadline-coll').catch(e => { caughtError = e; });
    
    // Mock simulates gRPC error after 60ms.
    await jest.advanceTimersByTimeAsync(60 + 1); 
    jest.runAllTicks();
    await Promise.resolve();
    jest.runAllTicks();
    await Promise.resolve();

    try {
      await promise;
    } catch (e) {
      if (!caughtError) caughtError = e;
    }

    expect(caughtError).toBeInstanceOf(VortexApiError);
    if (caughtError) {
      expect(caughtError.statusCode).toBe(grpc.status.DEADLINE_EXCEEDED);
    }
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });
  
  test('client-side timeout takes precedence if shorter than gRPC deadline', async () => {
    client = new VortexClient({ timeout: 200, requestTimeoutMs: 50, retriesEnabled: false }); // Client-side is shorter
    mockHangingCall(mockCollectionsStub.getCollectionInfo as jest.Mock);

    let caughtError: any = null;
    const promise = client.getCollectionInfoAsync('client-precedence-coll').catch(e => { caughtError = e; });
    
    await jest.advanceTimersByTimeAsync(50 + 1); 
    jest.runAllTicks();
    await Promise.resolve();
    jest.runAllTicks();
    await Promise.resolve();

    try {
      await promise;
    } catch (e) {
      if (!caughtError) caughtError = e;
    }

    expect(caughtError).toBeInstanceOf(VortexApiError);
    if (caughtError) {
      expect(caughtError.statusCode).toBe(grpc.status.CANCELLED);
      expect(caughtError.message).toContain('Request timed out client-side after 50ms');
      expect(caughtError.isClientTimeout).toBe(true);
    }
    expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
  });

  test('callback-style method should also timeout client-side', async () => {
    client = new VortexClient({ requestTimeoutMs: 75, retriesEnabled: false });
    mockHangingCall(mockCollectionsStub.getCollectionInfo as jest.Mock);

    const testPromise = new Promise<void>((resolve, reject) => {
      client.getCollectionInfo('cb-client-timeout-coll', (err, responseModel) => {
        try {
          expect(err).toBeDefined();
          expect(err!.code).toBe(grpc.status.CANCELLED);
          expect(err!.message).toContain('Request timed out client-side after 75ms');
          expect(responseModel).toBeNull();
          expect(mockCollectionsStub.getCollectionInfo).toHaveBeenCalledTimes(1);
          resolve();
        } catch (assertionError) {
          reject(assertionError);
        }
      });
    });
    
    await jest.advanceTimersByTimeAsync(75);
    await Promise.resolve(); // Flush microtasks
    await Promise.resolve(); // Flush microtasks again for safety
    await testPromise;
  });
});