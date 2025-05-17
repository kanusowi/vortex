// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var vortex_api_v1_points_service_pb = require('../../../vortex/api/v1/points_service_pb.js');
var vortex_api_v1_common_pb = require('../../../vortex/api/v1/common_pb.js');

function serialize_vortex_api_v1_DeletePointsRequest(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.DeletePointsRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.DeletePointsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_DeletePointsRequest(buffer_arg) {
  return vortex_api_v1_points_service_pb.DeletePointsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_DeletePointsResponse(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.DeletePointsResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.DeletePointsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_DeletePointsResponse(buffer_arg) {
  return vortex_api_v1_points_service_pb.DeletePointsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_GetPointsRequest(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.GetPointsRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.GetPointsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_GetPointsRequest(buffer_arg) {
  return vortex_api_v1_points_service_pb.GetPointsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_GetPointsResponse(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.GetPointsResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.GetPointsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_GetPointsResponse(buffer_arg) {
  return vortex_api_v1_points_service_pb.GetPointsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_SearchPointsRequest(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.SearchPointsRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.SearchPointsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_SearchPointsRequest(buffer_arg) {
  return vortex_api_v1_points_service_pb.SearchPointsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_SearchPointsResponse(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.SearchPointsResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.SearchPointsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_SearchPointsResponse(buffer_arg) {
  return vortex_api_v1_points_service_pb.SearchPointsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_UpsertPointsRequest(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.UpsertPointsRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.UpsertPointsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_UpsertPointsRequest(buffer_arg) {
  return vortex_api_v1_points_service_pb.UpsertPointsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_UpsertPointsResponse(arg) {
  if (!(arg instanceof vortex_api_v1_points_service_pb.UpsertPointsResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.UpsertPointsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_UpsertPointsResponse(buffer_arg) {
  return vortex_api_v1_points_service_pb.UpsertPointsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// Service for managing points (vectors and their payloads).
var PointsServiceService = exports.PointsServiceService = {
  // Upserts (adds or updates) points in a collection.
upsertPoints: {
    path: '/vortex.api.v1.PointsService/UpsertPoints',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_points_service_pb.UpsertPointsRequest,
    responseType: vortex_api_v1_points_service_pb.UpsertPointsResponse,
    requestSerialize: serialize_vortex_api_v1_UpsertPointsRequest,
    requestDeserialize: deserialize_vortex_api_v1_UpsertPointsRequest,
    responseSerialize: serialize_vortex_api_v1_UpsertPointsResponse,
    responseDeserialize: deserialize_vortex_api_v1_UpsertPointsResponse,
  },
  // Retrieves points by their IDs.
getPoints: {
    path: '/vortex.api.v1.PointsService/GetPoints',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_points_service_pb.GetPointsRequest,
    responseType: vortex_api_v1_points_service_pb.GetPointsResponse,
    requestSerialize: serialize_vortex_api_v1_GetPointsRequest,
    requestDeserialize: deserialize_vortex_api_v1_GetPointsRequest,
    responseSerialize: serialize_vortex_api_v1_GetPointsResponse,
    responseDeserialize: deserialize_vortex_api_v1_GetPointsResponse,
  },
  // Deletes points from a collection by their IDs.
deletePoints: {
    path: '/vortex.api.v1.PointsService/DeletePoints',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_points_service_pb.DeletePointsRequest,
    responseType: vortex_api_v1_points_service_pb.DeletePointsResponse,
    requestSerialize: serialize_vortex_api_v1_DeletePointsRequest,
    requestDeserialize: deserialize_vortex_api_v1_DeletePointsRequest,
    responseSerialize: serialize_vortex_api_v1_DeletePointsResponse,
    responseDeserialize: deserialize_vortex_api_v1_DeletePointsResponse,
  },
  // Performs a k-NN search for similar points.
searchPoints: {
    path: '/vortex.api.v1.PointsService/SearchPoints',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_points_service_pb.SearchPointsRequest,
    responseType: vortex_api_v1_points_service_pb.SearchPointsResponse,
    requestSerialize: serialize_vortex_api_v1_SearchPointsRequest,
    requestDeserialize: deserialize_vortex_api_v1_SearchPointsRequest,
    responseSerialize: serialize_vortex_api_v1_SearchPointsResponse,
    responseDeserialize: deserialize_vortex_api_v1_SearchPointsResponse,
  },
};

exports.PointsServiceClient = grpc.makeGenericClientConstructor(PointsServiceService, 'PointsService');
