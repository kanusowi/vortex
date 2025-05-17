// package: vortex.api.v1
// file: vortex/api/v1/collections_service.proto

var vortex_api_v1_collections_service_pb = require("../../../vortex/api/v1/collections_service_pb");
var grpc = require("@improbable-eng/grpc-web").grpc;

var CollectionsService = (function () {
  function CollectionsService() {}
  CollectionsService.serviceName = "vortex.api.v1.CollectionsService";
  return CollectionsService;
}());

CollectionsService.CreateCollection = {
  methodName: "CreateCollection",
  service: CollectionsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_collections_service_pb.CreateCollectionRequest,
  responseType: vortex_api_v1_collections_service_pb.CreateCollectionResponse
};

CollectionsService.GetCollectionInfo = {
  methodName: "GetCollectionInfo",
  service: CollectionsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_collections_service_pb.GetCollectionInfoRequest,
  responseType: vortex_api_v1_collections_service_pb.GetCollectionInfoResponse
};

CollectionsService.ListCollections = {
  methodName: "ListCollections",
  service: CollectionsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_collections_service_pb.ListCollectionsRequest,
  responseType: vortex_api_v1_collections_service_pb.ListCollectionsResponse
};

CollectionsService.DeleteCollection = {
  methodName: "DeleteCollection",
  service: CollectionsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_collections_service_pb.DeleteCollectionRequest,
  responseType: vortex_api_v1_collections_service_pb.DeleteCollectionResponse
};

exports.CollectionsService = CollectionsService;

function CollectionsServiceClient(serviceHost, options) {
  this.serviceHost = serviceHost;
  this.options = options || {};
}

CollectionsServiceClient.prototype.createCollection = function createCollection(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(CollectionsService.CreateCollection, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

CollectionsServiceClient.prototype.getCollectionInfo = function getCollectionInfo(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(CollectionsService.GetCollectionInfo, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

CollectionsServiceClient.prototype.listCollections = function listCollections(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(CollectionsService.ListCollections, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

CollectionsServiceClient.prototype.deleteCollection = function deleteCollection(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(CollectionsService.DeleteCollection, {
    request: requestMessage,
    host: this.serviceHost,
    metadata: metadata,
    transport: this.options.transport,
    debug: this.options.debug,
    onEnd: function (response) {
      if (callback) {
        if (response.status !== grpc.Code.OK) {
          var err = new Error(response.statusMessage);
          err.code = response.status;
          err.metadata = response.trailers;
          callback(err, null);
        } else {
          callback(null, response.message);
        }
      }
    }
  });
  return {
    cancel: function () {
      callback = null;
      client.close();
    }
  };
};

exports.CollectionsServiceClient = CollectionsServiceClient;

