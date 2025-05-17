// package: vortex.api.v1
// file: vortex/api/v1/points_service.proto

import * as jspb from "google-protobuf";
import * as vortex_api_v1_common_pb from "../../../vortex/api/v1/common_pb";

export class UpsertPointsRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  clearPointsList(): void;
  getPointsList(): Array<vortex_api_v1_common_pb.PointStruct>;
  setPointsList(value: Array<vortex_api_v1_common_pb.PointStruct>): void;
  addPoints(value?: vortex_api_v1_common_pb.PointStruct, index?: number): vortex_api_v1_common_pb.PointStruct;

  hasWaitFlush(): boolean;
  clearWaitFlush(): void;
  getWaitFlush(): boolean;
  setWaitFlush(value: boolean): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): UpsertPointsRequest.AsObject;
  static toObject(includeInstance: boolean, msg: UpsertPointsRequest): UpsertPointsRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: UpsertPointsRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): UpsertPointsRequest;
  static deserializeBinaryFromReader(message: UpsertPointsRequest, reader: jspb.BinaryReader): UpsertPointsRequest;
}

export namespace UpsertPointsRequest {
  export type AsObject = {
    collectionName: string,
    pointsList: Array<vortex_api_v1_common_pb.PointStruct.AsObject>,
    waitFlush: boolean,
  }
}

export class UpsertPointsResponse extends jspb.Message {
  clearStatusesList(): void;
  getStatusesList(): Array<vortex_api_v1_common_pb.PointOperationStatus>;
  setStatusesList(value: Array<vortex_api_v1_common_pb.PointOperationStatus>): void;
  addStatuses(value?: vortex_api_v1_common_pb.PointOperationStatus, index?: number): vortex_api_v1_common_pb.PointOperationStatus;

  hasOverallError(): boolean;
  clearOverallError(): void;
  getOverallError(): string;
  setOverallError(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): UpsertPointsResponse.AsObject;
  static toObject(includeInstance: boolean, msg: UpsertPointsResponse): UpsertPointsResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: UpsertPointsResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): UpsertPointsResponse;
  static deserializeBinaryFromReader(message: UpsertPointsResponse, reader: jspb.BinaryReader): UpsertPointsResponse;
}

export namespace UpsertPointsResponse {
  export type AsObject = {
    statusesList: Array<vortex_api_v1_common_pb.PointOperationStatus.AsObject>,
    overallError: string,
  }
}

export class GetPointsRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  clearIdsList(): void;
  getIdsList(): Array<string>;
  setIdsList(value: Array<string>): void;
  addIds(value: string, index?: number): string;

  hasWithPayload(): boolean;
  clearWithPayload(): void;
  getWithPayload(): boolean;
  setWithPayload(value: boolean): void;

  hasWithVector(): boolean;
  clearWithVector(): void;
  getWithVector(): boolean;
  setWithVector(value: boolean): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetPointsRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetPointsRequest): GetPointsRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetPointsRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetPointsRequest;
  static deserializeBinaryFromReader(message: GetPointsRequest, reader: jspb.BinaryReader): GetPointsRequest;
}

export namespace GetPointsRequest {
  export type AsObject = {
    collectionName: string,
    idsList: Array<string>,
    withPayload: boolean,
    withVector: boolean,
  }
}

export class GetPointsResponse extends jspb.Message {
  clearPointsList(): void;
  getPointsList(): Array<vortex_api_v1_common_pb.PointStruct>;
  setPointsList(value: Array<vortex_api_v1_common_pb.PointStruct>): void;
  addPoints(value?: vortex_api_v1_common_pb.PointStruct, index?: number): vortex_api_v1_common_pb.PointStruct;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetPointsResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetPointsResponse): GetPointsResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetPointsResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetPointsResponse;
  static deserializeBinaryFromReader(message: GetPointsResponse, reader: jspb.BinaryReader): GetPointsResponse;
}

export namespace GetPointsResponse {
  export type AsObject = {
    pointsList: Array<vortex_api_v1_common_pb.PointStruct.AsObject>,
  }
}

export class DeletePointsRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  clearIdsList(): void;
  getIdsList(): Array<string>;
  setIdsList(value: Array<string>): void;
  addIds(value: string, index?: number): string;

  hasWaitFlush(): boolean;
  clearWaitFlush(): void;
  getWaitFlush(): boolean;
  setWaitFlush(value: boolean): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeletePointsRequest.AsObject;
  static toObject(includeInstance: boolean, msg: DeletePointsRequest): DeletePointsRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeletePointsRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeletePointsRequest;
  static deserializeBinaryFromReader(message: DeletePointsRequest, reader: jspb.BinaryReader): DeletePointsRequest;
}

export namespace DeletePointsRequest {
  export type AsObject = {
    collectionName: string,
    idsList: Array<string>,
    waitFlush: boolean,
  }
}

export class DeletePointsResponse extends jspb.Message {
  clearStatusesList(): void;
  getStatusesList(): Array<vortex_api_v1_common_pb.PointOperationStatus>;
  setStatusesList(value: Array<vortex_api_v1_common_pb.PointOperationStatus>): void;
  addStatuses(value?: vortex_api_v1_common_pb.PointOperationStatus, index?: number): vortex_api_v1_common_pb.PointOperationStatus;

  hasOverallError(): boolean;
  clearOverallError(): void;
  getOverallError(): string;
  setOverallError(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeletePointsResponse.AsObject;
  static toObject(includeInstance: boolean, msg: DeletePointsResponse): DeletePointsResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeletePointsResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeletePointsResponse;
  static deserializeBinaryFromReader(message: DeletePointsResponse, reader: jspb.BinaryReader): DeletePointsResponse;
}

export namespace DeletePointsResponse {
  export type AsObject = {
    statusesList: Array<vortex_api_v1_common_pb.PointOperationStatus.AsObject>,
    overallError: string,
  }
}

export class SearchPointsRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  hasQueryVector(): boolean;
  clearQueryVector(): void;
  getQueryVector(): vortex_api_v1_common_pb.Vector | undefined;
  setQueryVector(value?: vortex_api_v1_common_pb.Vector): void;

  getKLimit(): number;
  setKLimit(value: number): void;

  hasFilter(): boolean;
  clearFilter(): void;
  getFilter(): vortex_api_v1_common_pb.Filter | undefined;
  setFilter(value?: vortex_api_v1_common_pb.Filter): void;

  hasWithPayload(): boolean;
  clearWithPayload(): void;
  getWithPayload(): boolean;
  setWithPayload(value: boolean): void;

  hasWithVector(): boolean;
  clearWithVector(): void;
  getWithVector(): boolean;
  setWithVector(value: boolean): void;

  hasParams(): boolean;
  clearParams(): void;
  getParams(): vortex_api_v1_common_pb.SearchParams | undefined;
  setParams(value?: vortex_api_v1_common_pb.SearchParams): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SearchPointsRequest.AsObject;
  static toObject(includeInstance: boolean, msg: SearchPointsRequest): SearchPointsRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: SearchPointsRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SearchPointsRequest;
  static deserializeBinaryFromReader(message: SearchPointsRequest, reader: jspb.BinaryReader): SearchPointsRequest;
}

export namespace SearchPointsRequest {
  export type AsObject = {
    collectionName: string,
    queryVector?: vortex_api_v1_common_pb.Vector.AsObject,
    kLimit: number,
    filter?: vortex_api_v1_common_pb.Filter.AsObject,
    withPayload: boolean,
    withVector: boolean,
    params?: vortex_api_v1_common_pb.SearchParams.AsObject,
  }
}

export class SearchPointsResponse extends jspb.Message {
  clearResultsList(): void;
  getResultsList(): Array<vortex_api_v1_common_pb.ScoredPoint>;
  setResultsList(value: Array<vortex_api_v1_common_pb.ScoredPoint>): void;
  addResults(value?: vortex_api_v1_common_pb.ScoredPoint, index?: number): vortex_api_v1_common_pb.ScoredPoint;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SearchPointsResponse.AsObject;
  static toObject(includeInstance: boolean, msg: SearchPointsResponse): SearchPointsResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: SearchPointsResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SearchPointsResponse;
  static deserializeBinaryFromReader(message: SearchPointsResponse, reader: jspb.BinaryReader): SearchPointsResponse;
}

export namespace SearchPointsResponse {
  export type AsObject = {
    resultsList: Array<vortex_api_v1_common_pb.ScoredPoint.AsObject>,
  }
}

