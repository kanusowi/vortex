// package: vortex.api.v1
// file: vortex/api/v1/points_service.proto

var vortex_api_v1_points_service_pb = require("../../../vortex/api/v1/points_service_pb");
var grpc = require("@improbable-eng/grpc-web").grpc;

var PointsService = (function () {
  function PointsService() {}
  PointsService.serviceName = "vortex.api.v1.PointsService";
  return PointsService;
}());

PointsService.UpsertPoints = {
  methodName: "UpsertPoints",
  service: PointsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_points_service_pb.UpsertPointsRequest,
  responseType: vortex_api_v1_points_service_pb.UpsertPointsResponse
};

PointsService.GetPoints = {
  methodName: "GetPoints",
  service: PointsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_points_service_pb.GetPointsRequest,
  responseType: vortex_api_v1_points_service_pb.GetPointsResponse
};

PointsService.DeletePoints = {
  methodName: "DeletePoints",
  service: PointsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_points_service_pb.DeletePointsRequest,
  responseType: vortex_api_v1_points_service_pb.DeletePointsResponse
};

PointsService.SearchPoints = {
  methodName: "SearchPoints",
  service: PointsService,
  requestStream: false,
  responseStream: false,
  requestType: vortex_api_v1_points_service_pb.SearchPointsRequest,
  responseType: vortex_api_v1_points_service_pb.SearchPointsResponse
};

exports.PointsService = PointsService;

function PointsServiceClient(serviceHost, options) {
  this.serviceHost = serviceHost;
  this.options = options || {};
}

PointsServiceClient.prototype.upsertPoints = function upsertPoints(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(PointsService.UpsertPoints, {
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

PointsServiceClient.prototype.getPoints = function getPoints(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(PointsService.GetPoints, {
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

PointsServiceClient.prototype.deletePoints = function deletePoints(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(PointsService.DeletePoints, {
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

PointsServiceClient.prototype.searchPoints = function searchPoints(requestMessage, metadata, callback) {
  if (arguments.length === 2) {
    callback = arguments[1];
  }
  var client = grpc.unary(PointsService.SearchPoints, {
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

exports.PointsServiceClient = PointsServiceClient;

