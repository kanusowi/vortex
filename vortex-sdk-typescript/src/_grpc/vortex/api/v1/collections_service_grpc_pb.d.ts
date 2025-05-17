import * as grpc from '@grpc/grpc-js';
import * as common_pb from './common_pb';
import * as collections_service_pb from './collections_service_pb';

export class CollectionsServiceClient extends grpc.Client {
  constructor(address: string, credentials?: grpc.ChannelCredentials, options?: grpc.ClientOptions);

  createCollection(
    request: collections_service_pb.CreateCollectionRequest,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.CreateCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;
  createCollection(
    request: collections_service_pb.CreateCollectionRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.CreateCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;
  createCollection(
    request: collections_service_pb.CreateCollectionRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.CreateCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;

  getCollectionInfo(
    request: collections_service_pb.GetCollectionInfoRequest,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.GetCollectionInfoResponse | null) => void
  ): grpc.ClientUnaryCall;
  getCollectionInfo(
    request: collections_service_pb.GetCollectionInfoRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.GetCollectionInfoResponse | null) => void
  ): grpc.ClientUnaryCall;
  getCollectionInfo(
    request: collections_service_pb.GetCollectionInfoRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.GetCollectionInfoResponse | null) => void
  ): grpc.ClientUnaryCall;

  listCollections(
    request: collections_service_pb.ListCollectionsRequest,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.ListCollectionsResponse | null) => void
  ): grpc.ClientUnaryCall;
  listCollections(
    request: collections_service_pb.ListCollectionsRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.ListCollectionsResponse | null) => void
  ): grpc.ClientUnaryCall;
  listCollections(
    request: collections_service_pb.ListCollectionsRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.ListCollectionsResponse | null) => void
  ): grpc.ClientUnaryCall;

  deleteCollection(
    request: collections_service_pb.DeleteCollectionRequest,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.DeleteCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;
  deleteCollection(
    request: collections_service_pb.DeleteCollectionRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.DeleteCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;
  deleteCollection(
    request: collections_service_pb.DeleteCollectionRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: collections_service_pb.DeleteCollectionResponse | null) => void
  ): grpc.ClientUnaryCall;
}
