/**
 * Custom exceptions for the Vortex TypeScript SDK.
 */
import * as grpc from '@grpc/grpc-js'; // Import the grpc namespace
import { ServiceError } from '@grpc/grpc-js';

/**
 * Base class for all Vortex SDK specific exceptions.
 */
export class VortexException extends Error {
  /**
   * Creates an instance of VortexException.
   * @param message - The error message.
   */
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

/**
 * Represents an error that occurs during connection to the Vortex server.
 */
export class VortexConnectionError extends VortexException {}

/**
 * Represents an error that occurs due to a timeout.
 */
export class VortexTimeoutError extends VortexException {}

/**
 * Represents an error returned by the Vortex API.
 * This class may wrap an underlying gRPC service error.
 */
export class VortexApiError extends VortexException {
  /**
   * The underlying gRPC ServiceError, if the API error originated from a gRPC call.
   */
  public readonly grpcError?: ServiceError;
  /**
   * The gRPC status code or a custom string status code associated with the API error.
   */
  public readonly statusCode?: grpc.status | string;
  /**
   * Additional details associated with the API error.
   */
  public readonly details?: string;
  /**
   * Flag indicating if this error was due to a client-side timeout.
   */
  public readonly isClientTimeout: boolean;

  /**
   * Creates an instance of VortexApiError.
   * @param message - The primary error message.
   * @param options - Optional parameters to include gRPC error details.
   * @param options.grpcError - The original gRPC ServiceError.
   * @param options.statusCode - A specific status code (gRPC or custom). Defaults to `grpcError.code`.
   * @param options.details - Additional error details. Defaults to `grpcError.details`.
   * @param options.isClientTimeout - Whether this error was due to a client-side timeout.
   */
  constructor(message: string, options?: {
    grpcError?: ServiceError;
    statusCode?: grpc.status | string;
    details?: string;
    isClientTimeout?: boolean;
  }) {
    super(message);
    this.grpcError = options?.grpcError;
    this.statusCode = options?.statusCode || options?.grpcError?.code;
    this.details = options?.details || options?.grpcError?.details;
    this.isClientTimeout = options?.isClientTimeout || false;
  }

  /**
   * Returns a string representation of the API error, including status code and details if available.
   * @returns A string describing the error.
   */
  public toString(): string {
    let str = super.toString();
    if (this.statusCode !== undefined) {
      str += ` (Status Code: ${this.statusCode})`;
    }
    if (this.details) {
      str += ` Details: ${this.details}`;
    }
    return str;
  }
}

/**
 * Represents an error related to client configuration.
 */
export class VortexClientConfigurationError extends VortexException {}
