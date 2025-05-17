// package: vortex.api.v1
// file: vortex/api/v1/collections_service.proto

import * as vortex_api_v1_collections_service_pb from "../../../vortex/api/v1/collections_service_pb";
import {grpc} from "@improbable-eng/grpc-web";

type CollectionsServiceCreateCollection = {
  readonly methodName: string;
  readonly service: typeof CollectionsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_collections_service_pb.CreateCollectionRequest;
  readonly responseType: typeof vortex_api_v1_collections_service_pb.CreateCollectionResponse;
};

type CollectionsServiceGetCollectionInfo = {
  readonly methodName: string;
  readonly service: typeof CollectionsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_collections_service_pb.GetCollectionInfoRequest;
  readonly responseType: typeof vortex_api_v1_collections_service_pb.GetCollectionInfoResponse;
};

type CollectionsServiceListCollections = {
  readonly methodName: string;
  readonly service: typeof CollectionsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_collections_service_pb.ListCollectionsRequest;
  readonly responseType: typeof vortex_api_v1_collections_service_pb.ListCollectionsResponse;
};

type CollectionsServiceDeleteCollection = {
  readonly methodName: string;
  readonly service: typeof CollectionsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_collections_service_pb.DeleteCollectionRequest;
  readonly responseType: typeof vortex_api_v1_collections_service_pb.DeleteCollectionResponse;
};

export class CollectionsService {
  static readonly serviceName: string;
  static readonly CreateCollection: CollectionsServiceCreateCollection;
  static readonly GetCollectionInfo: CollectionsServiceGetCollectionInfo;
  static readonly ListCollections: CollectionsServiceListCollections;
  static readonly DeleteCollection: CollectionsServiceDeleteCollection;
}

export type ServiceError = { message: string, code: number; metadata: grpc.Metadata }
export type Status = { details: string, code: number; metadata: grpc.Metadata }

interface UnaryResponse {
  cancel(): void;
}
interface ResponseStream<T> {
  cancel(): void;
  on(type: 'data', handler: (message: T) => void): ResponseStream<T>;
  on(type: 'end', handler: (status?: Status) => void): ResponseStream<T>;
  on(type: 'status', handler: (status: Status) => void): ResponseStream<T>;
}
interface RequestStream<T> {
  write(message: T): RequestStream<T>;
  end(): void;
  cancel(): void;
  on(type: 'end', handler: (status?: Status) => void): RequestStream<T>;
  on(type: 'status', handler: (status: Status) => void): RequestStream<T>;
}
interface BidirectionalStream<ReqT, ResT> {
  write(message: ReqT): BidirectionalStream<ReqT, ResT>;
  end(): void;
  cancel(): void;
  on(type: 'data', handler: (message: ResT) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'end', handler: (status?: Status) => void): BidirectionalStream<ReqT, ResT>;
  on(type: 'status', handler: (status: Status) => void): BidirectionalStream<ReqT, ResT>;
}

export class CollectionsServiceClient {
  readonly serviceHost: string;

  constructor(serviceHost: string, options?: grpc.RpcOptions);
  createCollection(
    requestMessage: vortex_api_v1_collections_service_pb.CreateCollectionRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.CreateCollectionResponse|null) => void
  ): UnaryResponse;
  createCollection(
    requestMessage: vortex_api_v1_collections_service_pb.CreateCollectionRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.CreateCollectionResponse|null) => void
  ): UnaryResponse;
  getCollectionInfo(
    requestMessage: vortex_api_v1_collections_service_pb.GetCollectionInfoRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.GetCollectionInfoResponse|null) => void
  ): UnaryResponse;
  getCollectionInfo(
    requestMessage: vortex_api_v1_collections_service_pb.GetCollectionInfoRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.GetCollectionInfoResponse|null) => void
  ): UnaryResponse;
  listCollections(
    requestMessage: vortex_api_v1_collections_service_pb.ListCollectionsRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.ListCollectionsResponse|null) => void
  ): UnaryResponse;
  listCollections(
    requestMessage: vortex_api_v1_collections_service_pb.ListCollectionsRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.ListCollectionsResponse|null) => void
  ): UnaryResponse;
  deleteCollection(
    requestMessage: vortex_api_v1_collections_service_pb.DeleteCollectionRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.DeleteCollectionResponse|null) => void
  ): UnaryResponse;
  deleteCollection(
    requestMessage: vortex_api_v1_collections_service_pb.DeleteCollectionRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_collections_service_pb.DeleteCollectionResponse|null) => void
  ): UnaryResponse;
}

