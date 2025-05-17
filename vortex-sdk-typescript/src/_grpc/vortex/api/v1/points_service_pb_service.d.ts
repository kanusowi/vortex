// package: vortex.api.v1
// file: vortex/api/v1/points_service.proto

import * as vortex_api_v1_points_service_pb from "../../../vortex/api/v1/points_service_pb";
import {grpc} from "@improbable-eng/grpc-web";

type PointsServiceUpsertPoints = {
  readonly methodName: string;
  readonly service: typeof PointsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_points_service_pb.UpsertPointsRequest;
  readonly responseType: typeof vortex_api_v1_points_service_pb.UpsertPointsResponse;
};

type PointsServiceGetPoints = {
  readonly methodName: string;
  readonly service: typeof PointsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_points_service_pb.GetPointsRequest;
  readonly responseType: typeof vortex_api_v1_points_service_pb.GetPointsResponse;
};

type PointsServiceDeletePoints = {
  readonly methodName: string;
  readonly service: typeof PointsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_points_service_pb.DeletePointsRequest;
  readonly responseType: typeof vortex_api_v1_points_service_pb.DeletePointsResponse;
};

type PointsServiceSearchPoints = {
  readonly methodName: string;
  readonly service: typeof PointsService;
  readonly requestStream: false;
  readonly responseStream: false;
  readonly requestType: typeof vortex_api_v1_points_service_pb.SearchPointsRequest;
  readonly responseType: typeof vortex_api_v1_points_service_pb.SearchPointsResponse;
};

export class PointsService {
  static readonly serviceName: string;
  static readonly UpsertPoints: PointsServiceUpsertPoints;
  static readonly GetPoints: PointsServiceGetPoints;
  static readonly DeletePoints: PointsServiceDeletePoints;
  static readonly SearchPoints: PointsServiceSearchPoints;
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

export class PointsServiceClient {
  readonly serviceHost: string;

  constructor(serviceHost: string, options?: grpc.RpcOptions);
  upsertPoints(
    requestMessage: vortex_api_v1_points_service_pb.UpsertPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.UpsertPointsResponse|null) => void
  ): UnaryResponse;
  upsertPoints(
    requestMessage: vortex_api_v1_points_service_pb.UpsertPointsRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.UpsertPointsResponse|null) => void
  ): UnaryResponse;
  getPoints(
    requestMessage: vortex_api_v1_points_service_pb.GetPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.GetPointsResponse|null) => void
  ): UnaryResponse;
  getPoints(
    requestMessage: vortex_api_v1_points_service_pb.GetPointsRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.GetPointsResponse|null) => void
  ): UnaryResponse;
  deletePoints(
    requestMessage: vortex_api_v1_points_service_pb.DeletePointsRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.DeletePointsResponse|null) => void
  ): UnaryResponse;
  deletePoints(
    requestMessage: vortex_api_v1_points_service_pb.DeletePointsRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.DeletePointsResponse|null) => void
  ): UnaryResponse;
  searchPoints(
    requestMessage: vortex_api_v1_points_service_pb.SearchPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.SearchPointsResponse|null) => void
  ): UnaryResponse;
  searchPoints(
    requestMessage: vortex_api_v1_points_service_pb.SearchPointsRequest,
    callback: (error: ServiceError|null, responseMessage: vortex_api_v1_points_service_pb.SearchPointsResponse|null) => void
  ): UnaryResponse;
}

