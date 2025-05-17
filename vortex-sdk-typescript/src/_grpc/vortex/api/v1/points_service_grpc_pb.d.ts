import * as grpc from '@grpc/grpc-js';
import * as common_pb from './common_pb';
import * as points_service_pb from './points_service_pb';

export class PointsServiceClient extends grpc.Client {
  constructor(address: string, credentials?: grpc.ChannelCredentials, options?: grpc.ClientOptions);

  upsertPoints(
    request: points_service_pb.UpsertPointsRequest,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.UpsertPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  upsertPoints(
    request: points_service_pb.UpsertPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.UpsertPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  upsertPoints(
    request: points_service_pb.UpsertPointsRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.UpsertPointsResponse | null) => void
  ): grpc.ClientUnaryCall;

  getPoints(
    request: points_service_pb.GetPointsRequest,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.GetPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  getPoints(
    request: points_service_pb.GetPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.GetPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  getPoints(
    request: points_service_pb.GetPointsRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.GetPointsResponse | null) => void
  ): grpc.ClientUnaryCall;

  deletePoints(
    request: points_service_pb.DeletePointsRequest,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.DeletePointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  deletePoints(
    request: points_service_pb.DeletePointsRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.DeletePointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  deletePoints(
    request: points_service_pb.DeletePointsRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.DeletePointsResponse | null) => void
  ): grpc.ClientUnaryCall;

  searchPoints(
    request: points_service_pb.SearchPointsRequest,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.SearchPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  searchPoints(
    request: points_service_pb.SearchPointsRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.SearchPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
  searchPoints(
    request: points_service_pb.SearchPointsRequest,
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response: points_service_pb.SearchPointsResponse | null) => void
  ): grpc.ClientUnaryCall;
}
