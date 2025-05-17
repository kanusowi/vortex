// package: vortex.api.v1
// file: vortex/api/v1/collections_service.proto

import * as jspb from "google-protobuf";
import * as vortex_api_v1_common_pb from "../../../vortex/api/v1/common_pb";

export class CreateCollectionRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  getVectorDimensions(): number;
  setVectorDimensions(value: number): void;

  getDistanceMetric(): vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap];
  setDistanceMetric(value: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap]): void;

  hasHnswConfig(): boolean;
  clearHnswConfig(): void;
  getHnswConfig(): vortex_api_v1_common_pb.HnswConfigParams | undefined;
  setHnswConfig(value?: vortex_api_v1_common_pb.HnswConfigParams): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CreateCollectionRequest.AsObject;
  static toObject(includeInstance: boolean, msg: CreateCollectionRequest): CreateCollectionRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: CreateCollectionRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CreateCollectionRequest;
  static deserializeBinaryFromReader(message: CreateCollectionRequest, reader: jspb.BinaryReader): CreateCollectionRequest;
}

export namespace CreateCollectionRequest {
  export type AsObject = {
    collectionName: string,
    vectorDimensions: number,
    distanceMetric: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap],
    hnswConfig?: vortex_api_v1_common_pb.HnswConfigParams.AsObject,
  }
}

export class CreateCollectionResponse extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CreateCollectionResponse.AsObject;
  static toObject(includeInstance: boolean, msg: CreateCollectionResponse): CreateCollectionResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: CreateCollectionResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CreateCollectionResponse;
  static deserializeBinaryFromReader(message: CreateCollectionResponse, reader: jspb.BinaryReader): CreateCollectionResponse;
}

export namespace CreateCollectionResponse {
  export type AsObject = {
  }
}

export class GetCollectionInfoRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetCollectionInfoRequest.AsObject;
  static toObject(includeInstance: boolean, msg: GetCollectionInfoRequest): GetCollectionInfoRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetCollectionInfoRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetCollectionInfoRequest;
  static deserializeBinaryFromReader(message: GetCollectionInfoRequest, reader: jspb.BinaryReader): GetCollectionInfoRequest;
}

export namespace GetCollectionInfoRequest {
  export type AsObject = {
    collectionName: string,
  }
}

export class GetCollectionInfoResponse extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  getStatus(): CollectionStatusMap[keyof CollectionStatusMap];
  setStatus(value: CollectionStatusMap[keyof CollectionStatusMap]): void;

  getVectorCount(): number;
  setVectorCount(value: number): void;

  getSegmentCount(): number;
  setSegmentCount(value: number): void;

  getDiskSizeBytes(): number;
  setDiskSizeBytes(value: number): void;

  getRamFootprintBytes(): number;
  setRamFootprintBytes(value: number): void;

  hasConfig(): boolean;
  clearConfig(): void;
  getConfig(): vortex_api_v1_common_pb.HnswConfigParams | undefined;
  setConfig(value?: vortex_api_v1_common_pb.HnswConfigParams): void;

  getDistanceMetric(): vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap];
  setDistanceMetric(value: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap]): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): GetCollectionInfoResponse.AsObject;
  static toObject(includeInstance: boolean, msg: GetCollectionInfoResponse): GetCollectionInfoResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: GetCollectionInfoResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): GetCollectionInfoResponse;
  static deserializeBinaryFromReader(message: GetCollectionInfoResponse, reader: jspb.BinaryReader): GetCollectionInfoResponse;
}

export namespace GetCollectionInfoResponse {
  export type AsObject = {
    collectionName: string,
    status: CollectionStatusMap[keyof CollectionStatusMap],
    vectorCount: number,
    segmentCount: number,
    diskSizeBytes: number,
    ramFootprintBytes: number,
    config?: vortex_api_v1_common_pb.HnswConfigParams.AsObject,
    distanceMetric: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap],
  }
}

export class ListCollectionsRequest extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ListCollectionsRequest.AsObject;
  static toObject(includeInstance: boolean, msg: ListCollectionsRequest): ListCollectionsRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ListCollectionsRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ListCollectionsRequest;
  static deserializeBinaryFromReader(message: ListCollectionsRequest, reader: jspb.BinaryReader): ListCollectionsRequest;
}

export namespace ListCollectionsRequest {
  export type AsObject = {
  }
}

export class ListCollectionsResponse extends jspb.Message {
  clearCollectionsList(): void;
  getCollectionsList(): Array<CollectionDescription>;
  setCollectionsList(value: Array<CollectionDescription>): void;
  addCollections(value?: CollectionDescription, index?: number): CollectionDescription;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ListCollectionsResponse.AsObject;
  static toObject(includeInstance: boolean, msg: ListCollectionsResponse): ListCollectionsResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ListCollectionsResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ListCollectionsResponse;
  static deserializeBinaryFromReader(message: ListCollectionsResponse, reader: jspb.BinaryReader): ListCollectionsResponse;
}

export namespace ListCollectionsResponse {
  export type AsObject = {
    collectionsList: Array<CollectionDescription.AsObject>,
  }
}

export class CollectionDescription extends jspb.Message {
  getName(): string;
  setName(value: string): void;

  getVectorCount(): number;
  setVectorCount(value: number): void;

  getStatus(): CollectionStatusMap[keyof CollectionStatusMap];
  setStatus(value: CollectionStatusMap[keyof CollectionStatusMap]): void;

  getDimensions(): number;
  setDimensions(value: number): void;

  getDistanceMetric(): vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap];
  setDistanceMetric(value: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap]): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): CollectionDescription.AsObject;
  static toObject(includeInstance: boolean, msg: CollectionDescription): CollectionDescription.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: CollectionDescription, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): CollectionDescription;
  static deserializeBinaryFromReader(message: CollectionDescription, reader: jspb.BinaryReader): CollectionDescription;
}

export namespace CollectionDescription {
  export type AsObject = {
    name: string,
    vectorCount: number,
    status: CollectionStatusMap[keyof CollectionStatusMap],
    dimensions: number,
    distanceMetric: vortex_api_v1_common_pb.DistanceMetricMap[keyof vortex_api_v1_common_pb.DistanceMetricMap],
  }
}

export class DeleteCollectionRequest extends jspb.Message {
  getCollectionName(): string;
  setCollectionName(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeleteCollectionRequest.AsObject;
  static toObject(includeInstance: boolean, msg: DeleteCollectionRequest): DeleteCollectionRequest.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeleteCollectionRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeleteCollectionRequest;
  static deserializeBinaryFromReader(message: DeleteCollectionRequest, reader: jspb.BinaryReader): DeleteCollectionRequest;
}

export namespace DeleteCollectionRequest {
  export type AsObject = {
    collectionName: string,
  }
}

export class DeleteCollectionResponse extends jspb.Message {
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): DeleteCollectionResponse.AsObject;
  static toObject(includeInstance: boolean, msg: DeleteCollectionResponse): DeleteCollectionResponse.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: DeleteCollectionResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): DeleteCollectionResponse;
  static deserializeBinaryFromReader(message: DeleteCollectionResponse, reader: jspb.BinaryReader): DeleteCollectionResponse;
}

export namespace DeleteCollectionResponse {
  export type AsObject = {
  }
}

export interface CollectionStatusMap {
  COLLECTION_STATUS_UNSPECIFIED: 0;
  GREEN: 1;
  YELLOW: 2;
  RED: 3;
  OPTIMIZING: 4;
  CREATING: 5;
}

export const CollectionStatus: CollectionStatusMap;

