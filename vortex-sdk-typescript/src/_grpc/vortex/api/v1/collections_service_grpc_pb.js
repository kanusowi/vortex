// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var vortex_api_v1_collections_service_pb = require('../../../vortex/api/v1/collections_service_pb.js');
var vortex_api_v1_common_pb = require('../../../vortex/api/v1/common_pb.js');

function serialize_vortex_api_v1_CreateCollectionRequest(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.CreateCollectionRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.CreateCollectionRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_CreateCollectionRequest(buffer_arg) {
  return vortex_api_v1_collections_service_pb.CreateCollectionRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_CreateCollectionResponse(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.CreateCollectionResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.CreateCollectionResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_CreateCollectionResponse(buffer_arg) {
  return vortex_api_v1_collections_service_pb.CreateCollectionResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_DeleteCollectionRequest(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.DeleteCollectionRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.DeleteCollectionRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_DeleteCollectionRequest(buffer_arg) {
  return vortex_api_v1_collections_service_pb.DeleteCollectionRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_DeleteCollectionResponse(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.DeleteCollectionResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.DeleteCollectionResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_DeleteCollectionResponse(buffer_arg) {
  return vortex_api_v1_collections_service_pb.DeleteCollectionResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_GetCollectionInfoRequest(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.GetCollectionInfoRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.GetCollectionInfoRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_GetCollectionInfoRequest(buffer_arg) {
  return vortex_api_v1_collections_service_pb.GetCollectionInfoRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_GetCollectionInfoResponse(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.GetCollectionInfoResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.GetCollectionInfoResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_GetCollectionInfoResponse(buffer_arg) {
  return vortex_api_v1_collections_service_pb.GetCollectionInfoResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_ListCollectionsRequest(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.ListCollectionsRequest)) {
    throw new Error('Expected argument of type vortex.api.v1.ListCollectionsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_ListCollectionsRequest(buffer_arg) {
  return vortex_api_v1_collections_service_pb.ListCollectionsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_vortex_api_v1_ListCollectionsResponse(arg) {
  if (!(arg instanceof vortex_api_v1_collections_service_pb.ListCollectionsResponse)) {
    throw new Error('Expected argument of type vortex.api.v1.ListCollectionsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_vortex_api_v1_ListCollectionsResponse(buffer_arg) {
  return vortex_api_v1_collections_service_pb.ListCollectionsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}


// Service for managing collections (indices).
var CollectionsServiceService = exports.CollectionsServiceService = {
  // Creates a new collection.
createCollection: {
    path: '/vortex.api.v1.CollectionsService/CreateCollection',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_collections_service_pb.CreateCollectionRequest,
    responseType: vortex_api_v1_collections_service_pb.CreateCollectionResponse,
    requestSerialize: serialize_vortex_api_v1_CreateCollectionRequest,
    requestDeserialize: deserialize_vortex_api_v1_CreateCollectionRequest,
    responseSerialize: serialize_vortex_api_v1_CreateCollectionResponse,
    responseDeserialize: deserialize_vortex_api_v1_CreateCollectionResponse,
  },
  // Gets detailed information about a collection.
getCollectionInfo: {
    path: '/vortex.api.v1.CollectionsService/GetCollectionInfo',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_collections_service_pb.GetCollectionInfoRequest,
    responseType: vortex_api_v1_collections_service_pb.GetCollectionInfoResponse,
    requestSerialize: serialize_vortex_api_v1_GetCollectionInfoRequest,
    requestDeserialize: deserialize_vortex_api_v1_GetCollectionInfoRequest,
    responseSerialize: serialize_vortex_api_v1_GetCollectionInfoResponse,
    responseDeserialize: deserialize_vortex_api_v1_GetCollectionInfoResponse,
  },
  // Lists all available collections.
listCollections: {
    path: '/vortex.api.v1.CollectionsService/ListCollections',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_collections_service_pb.ListCollectionsRequest,
    responseType: vortex_api_v1_collections_service_pb.ListCollectionsResponse,
    requestSerialize: serialize_vortex_api_v1_ListCollectionsRequest,
    requestDeserialize: deserialize_vortex_api_v1_ListCollectionsRequest,
    responseSerialize: serialize_vortex_api_v1_ListCollectionsResponse,
    responseDeserialize: deserialize_vortex_api_v1_ListCollectionsResponse,
  },
  // Deletes a collection and all its data.
deleteCollection: {
    path: '/vortex.api.v1.CollectionsService/DeleteCollection',
    requestStream: false,
    responseStream: false,
    requestType: vortex_api_v1_collections_service_pb.DeleteCollectionRequest,
    responseType: vortex_api_v1_collections_service_pb.DeleteCollectionResponse,
    requestSerialize: serialize_vortex_api_v1_DeleteCollectionRequest,
    requestDeserialize: deserialize_vortex_api_v1_DeleteCollectionRequest,
    responseSerialize: serialize_vortex_api_v1_DeleteCollectionResponse,
    responseDeserialize: deserialize_vortex_api_v1_DeleteCollectionResponse,
  },
};

exports.CollectionsServiceClient = grpc.makeGenericClientConstructor(CollectionsServiceService, 'CollectionsService');
