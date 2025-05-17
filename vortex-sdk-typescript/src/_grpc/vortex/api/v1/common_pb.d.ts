// package: vortex.api.v1
// file: vortex/api/v1/common.proto

import * as jspb from "google-protobuf";
import * as google_protobuf_struct_pb from "google-protobuf/google/protobuf/struct_pb";

export class Vector extends jspb.Message {
  clearElementsList(): void;
  getElementsList(): Array<number>;
  setElementsList(value: Array<number>): void;
  addElements(value: number, index?: number): number;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Vector.AsObject;
  static toObject(includeInstance: boolean, msg: Vector): Vector.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Vector, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Vector;
  static deserializeBinaryFromReader(message: Vector, reader: jspb.BinaryReader): Vector;
}

export namespace Vector {
  export type AsObject = {
    elementsList: Array<number>,
  }
}

export class Payload extends jspb.Message {
  getFieldsMap(): jspb.Map<string, google_protobuf_struct_pb.Value>;
  clearFieldsMap(): void;
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Payload.AsObject;
  static toObject(includeInstance: boolean, msg: Payload): Payload.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Payload, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Payload;
  static deserializeBinaryFromReader(message: Payload, reader: jspb.BinaryReader): Payload;
}

export namespace Payload {
  export type AsObject = {
    fieldsMap: Array<[string, google_protobuf_struct_pb.Value.AsObject]>,
  }
}

export class PointStruct extends jspb.Message {
  getId(): string;
  setId(value: string): void;

  hasVector(): boolean;
  clearVector(): void;
  getVector(): Vector | undefined;
  setVector(value?: Vector): void;

  hasPayload(): boolean;
  clearPayload(): void;
  getPayload(): Payload | undefined;
  setPayload(value?: Payload): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): PointStruct.AsObject;
  static toObject(includeInstance: boolean, msg: PointStruct): PointStruct.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: PointStruct, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): PointStruct;
  static deserializeBinaryFromReader(message: PointStruct, reader: jspb.BinaryReader): PointStruct;
}

export namespace PointStruct {
  export type AsObject = {
    id: string,
    vector?: Vector.AsObject,
    payload?: Payload.AsObject,
  }
}

export class ScoredPoint extends jspb.Message {
  getId(): string;
  setId(value: string): void;

  hasVector(): boolean;
  clearVector(): void;
  getVector(): Vector | undefined;
  setVector(value?: Vector): void;

  hasPayload(): boolean;
  clearPayload(): void;
  getPayload(): Payload | undefined;
  setPayload(value?: Payload): void;

  getScore(): number;
  setScore(value: number): void;

  hasVersion(): boolean;
  clearVersion(): void;
  getVersion(): number;
  setVersion(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): ScoredPoint.AsObject;
  static toObject(includeInstance: boolean, msg: ScoredPoint): ScoredPoint.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: ScoredPoint, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): ScoredPoint;
  static deserializeBinaryFromReader(message: ScoredPoint, reader: jspb.BinaryReader): ScoredPoint;
}

export namespace ScoredPoint {
  export type AsObject = {
    id: string,
    vector?: Vector.AsObject,
    payload?: Payload.AsObject,
    score: number,
    version: number,
  }
}

export class Filter extends jspb.Message {
  getMustMatchExactMap(): jspb.Map<string, google_protobuf_struct_pb.Value>;
  clearMustMatchExactMap(): void;
  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): Filter.AsObject;
  static toObject(includeInstance: boolean, msg: Filter): Filter.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: Filter, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): Filter;
  static deserializeBinaryFromReader(message: Filter, reader: jspb.BinaryReader): Filter;
}

export namespace Filter {
  export type AsObject = {
    mustMatchExactMap: Array<[string, google_protobuf_struct_pb.Value.AsObject]>,
  }
}

export class HnswConfigParams extends jspb.Message {
  getM(): number;
  setM(value: number): void;

  getEfConstruction(): number;
  setEfConstruction(value: number): void;

  getEfSearch(): number;
  setEfSearch(value: number): void;

  getMl(): number;
  setMl(value: number): void;

  hasSeed(): boolean;
  clearSeed(): void;
  getSeed(): number;
  setSeed(value: number): void;

  getVectorDim(): number;
  setVectorDim(value: number): void;

  getMMax0(): number;
  setMMax0(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): HnswConfigParams.AsObject;
  static toObject(includeInstance: boolean, msg: HnswConfigParams): HnswConfigParams.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: HnswConfigParams, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): HnswConfigParams;
  static deserializeBinaryFromReader(message: HnswConfigParams, reader: jspb.BinaryReader): HnswConfigParams;
}

export namespace HnswConfigParams {
  export type AsObject = {
    m: number,
    efConstruction: number,
    efSearch: number,
    ml: number,
    seed: number,
    vectorDim: number,
    mMax0: number,
  }
}

export class SearchParams extends jspb.Message {
  hasEfSearch(): boolean;
  clearEfSearch(): void;
  getEfSearch(): number;
  setEfSearch(value: number): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): SearchParams.AsObject;
  static toObject(includeInstance: boolean, msg: SearchParams): SearchParams.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: SearchParams, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): SearchParams;
  static deserializeBinaryFromReader(message: SearchParams, reader: jspb.BinaryReader): SearchParams;
}

export namespace SearchParams {
  export type AsObject = {
    efSearch: number,
  }
}

export class PointOperationStatus extends jspb.Message {
  getPointId(): string;
  setPointId(value: string): void;

  getStatusCode(): StatusCodeMap[keyof StatusCodeMap];
  setStatusCode(value: StatusCodeMap[keyof StatusCodeMap]): void;

  hasErrorMessage(): boolean;
  clearErrorMessage(): void;
  getErrorMessage(): string;
  setErrorMessage(value: string): void;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): PointOperationStatus.AsObject;
  static toObject(includeInstance: boolean, msg: PointOperationStatus): PointOperationStatus.AsObject;
  static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
  static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
  static serializeBinaryToWriter(message: PointOperationStatus, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): PointOperationStatus;
  static deserializeBinaryFromReader(message: PointOperationStatus, reader: jspb.BinaryReader): PointOperationStatus;
}

export namespace PointOperationStatus {
  export type AsObject = {
    pointId: string,
    statusCode: StatusCodeMap[keyof StatusCodeMap],
    errorMessage: string,
  }
}

export interface DistanceMetricMap {
  DISTANCE_METRIC_UNSPECIFIED: 0;
  COSINE: 1;
  EUCLIDEAN_L2: 2;
}

export const DistanceMetric: DistanceMetricMap;

export interface StatusCodeMap {
  STATUS_CODE_UNSPECIFIED: 0;
  OK: 1;
  ERROR: 2;
  NOT_FOUND: 3;
  INVALID_ARGUMENT: 4;
}

export const StatusCode: StatusCodeMap;

