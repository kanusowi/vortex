/**
 * @fileoverview Main client for interacting with the Vortex Vector Database.
 * This file defines the `VortexClient` class, its configuration options,
 * and methods for all supported gRPC operations.
 */
import * as grpc from '@grpc/grpc-js';
import * as models from './models';
import * as conversions from './conversions';
import { VortexApiError } from './exceptions';

// Generated gRPC client stubs
import { CollectionsServiceClient } from './_grpc/vortex/api/v1/collections_service_grpc_pb';
import { PointsServiceClient } from './_grpc/vortex/api/v1/points_service_grpc_pb';

// Generated gRPC message types
import * as collections_service_pb from './_grpc/vortex/api/v1/collections_service_pb';
import * as points_service_pb from './_grpc/vortex/api/v1/points_service_pb';

/**
 * Configuration options for the {@link VortexClient}.
 */
export interface VortexClientOptions {
  /**
   * The full URL of the Vortex server, including protocol, host, port, and optional path prefix.
   * Example: "http://localhost:50051", "https://my-vortex.com/api-prefix".
   * If provided, `host` and `port` options are ignored.
   */
  url?: string;
  /**
   * The hostname or IP address of the Vortex server.
   * Example: "localhost", "my-vortex.com".
   * Should not include protocol or port. Use `url` for that.
   * Defaults to "localhost" if neither `url` nor `host` is provided.
   */
  host?: string;
  /**
   * The port number of the Vortex server.
   * Example: 50051.
   * Defaults to 50051 for insecure connections or 443 for secure connections if not specified.
   */
  port?: number;
  /** Optional API key for authentication with the Vortex server. */
  apiKey?: string;
  /**
   * Optional timeout in milliseconds for gRPC deadlines.
   * This sets a deadline on the gRPC call itself.
   */
  timeout?: number;
  /**
   * Flag to enable SSL/TLS for a secure connection.
   * Automatically inferred from `url` if it starts with "https://".
   * Defaults to `false` if not specified and URL is not "https://".
   */
  secure?: boolean;
  /** Optional Root CA certificates for SSL/TLS connections. */
  rootCerts?: Buffer;
  /** Optional client private key for mutual TLS (mTLS) connections. */
  privateKey?: Buffer;
  /** Optional client certificate chain for mutual TLS (mTLS) connections. */
  certChain?: Buffer;
  /**
   * Optional gRPC channel options to pass through to the underlying gRPC client.
   * See `@grpc/grpc-js` documentation for available options.
   */
  grpcClientOptions?: grpc.ChannelOptions;
  /**
   * Optional client-side request timeout in milliseconds.
   * If a request does not complete within this duration, it will be cancelled.
   * This complements the gRPC `timeout` (deadline) option.
   */
  requestTimeoutMs?: number;

  // Retry configuration options
  /**
   * Whether to enable automatic retries for failed operations.
   * Defaults to `true`.
   */
  retriesEnabled?: boolean;
  /**
   * Maximum number of retry attempts for a failed operation.
   * Defaults to `3`.
   */
  maxRetries?: number;
  /**
   * Initial backoff delay in milliseconds before the first retry.
   * Defaults to `200`.
   */
  initialBackoffMs?: number;
  /**
   * Maximum backoff delay in milliseconds between retries.
   * Defaults to `5000`.
   */
  maxBackoffMs?: number;
  /**
   * Multiplier for increasing the backoff delay exponentially.
   * Defaults to `1.5`.
   */
  backoffMultiplier?: number;
  /**
   * Jitter factor (percentage) to apply to backoff delays, e.g., 0.1 for 10%.
   * Helps prevent thundering herd problems. Defaults to `0.1`.
   */
  retryJitter?: number;
  /**
   * Array of gRPC status codes that are considered retryable.
   * Defaults to `[grpc.status.UNAVAILABLE, grpc.status.RESOURCE_EXHAUSTED]`.
   */
  retryableStatusCodes?: grpc.status[];
}

/**
 * The main client class for interacting with a Vortex Vector Database.
 * Provides methods for managing collections and points, with support for
 * both callback-style and Promise-based asynchronous operations.
 *
 * @example
 * ```typescript
 * import { VortexClient, DistanceMetric } from 'vortex-sdk-typescript';
 *
 * // Connect using host and port
 * const client1 = new VortexClient({ host: 'localhost', port: 50051 });
 *
 * // Connect using URL
 * const client2 = new VortexClient({ url: 'http://localhost:50051' });
 *
 * // Connect with API key and secure connection
 * const client3 = new VortexClient({
 *   url: 'https://secure.vortex-server.com',
 *   apiKey: 'your-api-key',
 * });
 *
 * async function main() {
 *   try {
 *     await client1.createCollectionAsync('my-collection', 128, DistanceMetric.COSINE);
 *     console.log('Collection created');
 *     const collections = await client1.listCollectionsAsync();
 *     console.log('Collections:', collections);
 *   } catch (error) {
 *     console.error('Vortex API Error:', error);
 *   } finally {
 *     client1.close();
 *   }
 * }
 * main();
 * ```
 */
export class VortexClient {
  /**
   * @internal
   * Stores the processed and defaulted client options.
   */
  private clientOptions: {
    host: string;
    port: number;
    prefix: string;
    apiKey?: string;
    timeout?: number;
    requestTimeoutMs?: number;
    secure: boolean;
    rootCerts?: Buffer;
    privateKey?: Buffer;
    certChain?: Buffer;
    grpcClientOptions?: grpc.ChannelOptions;

    // Retry configuration
    retriesEnabled: boolean;
    maxRetries: number;
    initialBackoffMs: number;
    maxBackoffMs: number;
    backoffMultiplier: number;
    retryJitter: number;
    retryableStatusCodes: grpc.status[];
  };

  /** @internal */
  private _collectionsStub: CollectionsServiceClient | null = null;
  /** @internal */
  private _pointsStub: PointsServiceClient | null = null;
  /** @internal */
  private readonly sdkVersion = "0.1.0"; // TODO: Consider dynamically getting this from package.json

  /**
   * Creates an instance of VortexClient.
   * @param {VortexClientOptions} [options={}] - Configuration options for the client.
   * @throws {VortexApiError} If configuration options are invalid (e.g., conflicting `url` and `host`/`port`, malformed `url` or `host`).
   */
  constructor(options: VortexClientOptions = {}) {
    let host = options.host;
    let port = options.port;
    let secure = options.secure === undefined ? (options.url?.startsWith('https://') || false) : options.secure;
    let prefix = '';

    if (options.url && (options.host || options.port !== undefined)) {
      throw new VortexApiError('VortexClientConfigError: Only one of `url` or `host`/`port` params can be set.', { statusCode: grpc.status.INVALID_ARGUMENT });
    }

    if (options.host && (options.host.startsWith('http://') || options.host.startsWith('https://') || /:\d+$/.test(options.host))) {
      throw new VortexApiError('VortexClientConfigError: The `host` param should not contain protocol or port. Use `url` instead.', { statusCode: grpc.status.INVALID_ARGUMENT });
    }
    
    if (options.url) {
      if (!(options.url.startsWith('http://') || options.url.startsWith('https://'))) {
        throw new VortexApiError('VortexClientConfigError: The `url` param must start with http:// or https://.', { statusCode: grpc.status.INVALID_ARGUMENT });
      }
      try {
        const parsedUrl = new URL(options.url);
        host = parsedUrl.hostname;
        port = parsedUrl.port ? Number(parsedUrl.port) : (parsedUrl.protocol === 'https:' ? 443 : 80);
        secure = parsedUrl.protocol === 'https:';
        if (parsedUrl.pathname && parsedUrl.pathname !== '/') {
            prefix = parsedUrl.pathname.replace(/\/$/, ''); 
        }
      } catch (e) {
        throw new VortexApiError(`VortexClientConfigError: Invalid URL: ${(e as Error).message}`, { statusCode: grpc.status.INVALID_ARGUMENT });
      }
    }
    
    if (!host) {
        host = 'localhost';
    }
    if (port === undefined) {
        port = secure ? 443 : 50051; 
    }

    if (options.apiKey && !secure) {
      console.warn('VortexClient: API key is used with an insecure connection (secure=false or http:// URL).');
    }

    this.clientOptions = {
      host: host,
      port: port,
      prefix: prefix, 
      apiKey: options.apiKey,
      timeout: options.timeout,
      requestTimeoutMs: options.requestTimeoutMs,
      secure: secure,
      rootCerts: options.rootCerts,
      privateKey: options.privateKey,
      certChain: options.certChain,
      grpcClientOptions: options.grpcClientOptions,

      retriesEnabled: options.retriesEnabled === undefined ? true : options.retriesEnabled,
      maxRetries: options.maxRetries === undefined ? 3 : options.maxRetries,
      initialBackoffMs: options.initialBackoffMs === undefined ? 200 : options.initialBackoffMs,
      maxBackoffMs: options.maxBackoffMs === undefined ? 5000 : options.maxBackoffMs,
      backoffMultiplier: options.backoffMultiplier === undefined ? 1.5 : options.backoffMultiplier,
      retryJitter: options.retryJitter === undefined ? 0.1 : options.retryJitter,
      retryableStatusCodes: options.retryableStatusCodes || [
        grpc.status.UNAVAILABLE,
        grpc.status.RESOURCE_EXHAUSTED,
      ],
    };
    this.connect();
  }

  /**
   * Prepares gRPC metadata for requests, including user-agent and API key if configured.
   * @returns {grpc.Metadata} The gRPC metadata object.
   * @internal
   */
  private _prepareMetadata(): grpc.Metadata {
    const metadata = new grpc.Metadata();
    metadata.set('user-agent', `vortex-sdk-ts/${this.sdkVersion}`);
    if (this.clientOptions.apiKey) {
      metadata.set('api-key', this.clientOptions.apiKey);
    }
    // TODO: Add prefix to metadata if needed by an envoy proxy or similar for path-based routing.
    // if (this.clientOptions.prefix) {
    //   metadata.set('x-vortex-path-prefix', this.clientOptions.prefix);
    // }
    return metadata;
  }

  /**
   * Internal helper method to execute a gRPC unary call with support for client-side timeouts and automatic retries.
   * @template Req - The type of the gRPC request message.
   * @template Res - The type of the gRPC response message.
   * @template ConvertedRes - The type of the response after conversion (if a converter is provided).
   * @param {Function} grpcCall - The gRPC client method to call (e.g., `this._collectionsStub.createCollection.bind(this._collectionsStub)`).
   * @param {Req} request - The gRPC request message.
   * @param {string} operationName - A descriptive name of the operation for logging and error messages.
   * @param {(err: grpc.ServiceError | null, response: ConvertedRes | null) => void} [finalCallback] - Optional callback for non-Promise calls.
   * @param {(grpcResponse: Res) => ConvertedRes} [responseConverter] - Optional function to convert gRPC response to SDK model.
   * @returns {Promise<Res>} A promise that resolves with the gRPC response or rejects with an error.
   * @throws {VortexApiError} If the operation fails after retries or due to a non-retryable error.
   * @internal
   */
  private async _execute_with_retry_ts<Req, Res, ConvertedRes = Res>(
    grpcCall: (request: Req, metadata: grpc.Metadata, options: grpc.CallOptions, callback: (error: grpc.ServiceError | null, response: Res | null) => void) => grpc.ClientUnaryCall,
    request: Req,
    operationName: string,
    finalCallback?: (err: grpc.ServiceError | null, response: ConvertedRes | null) => void,
    responseConverter?: (grpcResponse: Res) => ConvertedRes
  ): Promise<Res> {
    console.log(`[${operationName}] Entering _execute_with_retry_ts. Retries enabled: ${this.clientOptions.retriesEnabled}`);
    const executeSingleCall = (): Promise<Res> => {
      return new Promise<Res>((resolve, reject) => {
        let isSettled = false; // Flag to ensure promise is settled only once

        let callOptions: grpc.CallOptions = {};
        if (this.clientOptions.timeout) { // gRPC deadline
          callOptions.deadline = new Date(Date.now() + this.clientOptions.timeout);
        }

        let timeoutId: NodeJS.Timeout | undefined;
        let grpcUnaryCallInstance: grpc.ClientUnaryCall | undefined;
        const controller = this.clientOptions.requestTimeoutMs ? new AbortController() : undefined;
        
        const metadata = this._prepareMetadata();

        // This function will be called when the AbortController signals an abort.
        const handleAbort = () => {
            if (isSettled) return;
            isSettled = true;
            console.log(`[${operationName}] Client-side timeout triggered (handleAbort). Cancelling gRPC call.`);
            grpcUnaryCallInstance?.cancel(); 
            
            const grpcTimeoutErrorSim: grpc.ServiceError = {
                code: grpc.status.CANCELLED,
                message: `Request timed out client-side after ${this.clientOptions.requestTimeoutMs}ms`,
                details: 'Client-side timeout',
                name: 'ClientTimeoutError',
                metadata: new grpc.Metadata(),
            };
            const clientTimeoutError = new VortexApiError(
                grpcTimeoutErrorSim.message,
                { statusCode: grpc.status.CANCELLED, details: 'Client-side timeout', grpcError: grpcTimeoutErrorSim, isClientTimeout: true } as any
            );
            console.log(`[${operationName}] Rejecting promise due to client-side timeout with error:`, JSON.stringify(clientTimeoutError, null, 2));
            if (timeoutId) clearTimeout(timeoutId); // Clear timeout as we are handling it
            if (controller) controller.signal.removeEventListener('abort', handleAbort);
            reject(clientTimeoutError);
        };

        if (controller && this.clientOptions.requestTimeoutMs) {
          console.log(`[${operationName}] Setting up client-side timeout: ${this.clientOptions.requestTimeoutMs}ms`);
          controller.signal.addEventListener('abort', handleAbort, { once: true });
          timeoutId = setTimeout(() => {
            console.log(`[${operationName}] Client-side timeout setTimeout fired.`);
            if (!controller.signal.aborted && !isSettled) {
                console.log(`[${operationName}] AbortController not yet aborted, calling controller.abort().`);
                controller.abort();
            } else {
                console.log(`[${operationName}] AbortController already aborted or promise settled when setTimeout fired.`);
            }
          }, this.clientOptions.requestTimeoutMs);
        }
        
        console.log(`[${operationName}] Making gRPC call.`);
        grpcUnaryCallInstance = grpcCall(request, metadata, callOptions, (err, response) => {
          if (isSettled) {
            console.log(`[${operationName}] gRPC callback invoked, but promise already settled. Ignoring.`);
            return;
          }
          isSettled = true;
          console.log(`[${operationName}] gRPC callback invoked. Error: ${err ? err.code : 'null'}, Response: ${response ? 'present' : 'null'}`);
          
          if (timeoutId) {
            console.log(`[${operationName}] Clearing client-side timeoutId.`);
            clearTimeout(timeoutId);
          }
          if (controller) {
            console.log(`[${operationName}] Removing abort listener.`);
            controller.signal.removeEventListener('abort', handleAbort);
          }

          // If controller.abort() was called by our timeout, handleAbort would have set isClientTimeout=true on the error.
          // If the gRPC call itself was cancelled for other reasons, or genuinely timed out on the server (DEADLINE_EXCEEDED),
          // it would come here.
          if (err) {
            console.log(`[${operationName}] gRPC call failed. Error code: ${err.code}, message: ${err.message}`);
            // Check if this cancellation was due to our client-side timeout mechanism
            if (err.code === grpc.status.CANCELLED && controller?.signal.aborted) {
                 console.log(`[${operationName}] gRPC call cancelled, likely due to client-side timeout. Constructing specific error.`);
                 const clientTimeoutError = new VortexApiError(
                    `Request timed out client-side after ${this.clientOptions.requestTimeoutMs}ms (gRPC CANCELLED)`,
                    { statusCode: grpc.status.CANCELLED, details: 'Client-side timeout via gRPC CANCELLED', grpcError: err, isClientTimeout: true } as any
                );
                reject(clientTimeoutError);
            } else {
                reject(err); 
            }
          } else if (response) {
            console.log(`[${operationName}] gRPC call successful.`);
            resolve(response);
          } else {
            console.log(`[${operationName}] gRPC call returned no error and no response.`);
            reject(new VortexApiError(`No response or error from ${operationName}`, { statusCode: grpc.status.UNKNOWN }));
          }
        });
      });
    };

    if (!this.clientOptions.retriesEnabled) {
      console.log(`[${operationName}] Retries disabled. Executing single call.`);
      try {
        const response = await executeSingleCall();
        if (finalCallback) {
          const converted = responseConverter ? responseConverter(response) : response as unknown as ConvertedRes;
          finalCallback(null, converted);
        }
        return response;
      } catch (error) {
        console.log(`[${operationName}] Error in single call (retries disabled):`, error);
        if (error instanceof VortexApiError && (error as any).isClientTimeout) {
            console.log(`[${operationName}] Client-side timeout error (retries disabled).`);
            if (finalCallback) finalCallback(error.grpcError || { code: grpc.status.CANCELLED, message: error.message, details: error.details || error.message, metadata: new grpc.Metadata(), name: 'VortexClientTimeout' } as grpc.ServiceError, null);
            throw error;
        }
        const serviceError = error instanceof VortexApiError ? 
            (error.grpcError || { code: error.statusCode || grpc.status.UNKNOWN, message: error.message, details: error.details || error.message, metadata: new grpc.Metadata(), name: 'VortexApiErrorWrapper' } as grpc.ServiceError)
            : error as grpc.ServiceError;
        if (finalCallback) finalCallback(serviceError, null);
        if (error instanceof VortexApiError) throw error; 
        throw new VortexApiError(`Failed to ${operationName} (retries disabled). Error: ${serviceError.message}`, { statusCode: serviceError.code, details: serviceError.details, grpcError: serviceError });
      }
    }
    
    
    let attempts = 0;
    let currentBackoffMs = this.clientOptions.initialBackoffMs;
    console.log(`[${operationName}] Starting retry loop. Max retries: ${this.clientOptions.maxRetries}, Initial backoff: ${currentBackoffMs}ms`);

    while (attempts <= this.clientOptions.maxRetries) {
      console.log(`[${operationName}] Retry attempt #${attempts + 1}`);
      try {
        const result = await executeSingleCall();
        console.log(`[${operationName}] Attempt #${attempts + 1} successful.`);
        if (finalCallback) {
          const converted = responseConverter ? responseConverter(result) : result as unknown as ConvertedRes;
          finalCallback(null, converted);
        }
        return result;
      } catch (error) { 
        attempts++;
        console.log(`[${operationName}] Attempt #${attempts} failed. Error:`, error);
        let errorForCallback: grpc.ServiceError;
        let errorToThrow: VortexApiError;
        // Correctly determine if the caught error is our specific client-side timeout error
        const isClientTimeoutError = error instanceof VortexApiError && error.isClientTimeout === true;

        if (isClientTimeoutError) {
            console.log(`[${operationName}] Caught error IS a client-side timeout. Flag: ${error.isClientTimeout}`);
            errorForCallback = error.grpcError || 
                               { code: grpc.status.CANCELLED, message: error.message, details: error.details || 'Client-side timeout', metadata: new grpc.Metadata(), name: 'ClientTimeoutErrorSimulatedGrpcError' } as grpc.ServiceError;
            errorToThrow = error; // Preserve the original client timeout error, which has the flag
        } else if (error instanceof VortexApiError && error.grpcError) {
            console.log(`[${operationName}] Caught error is VortexApiError with grpcError (but not client timeout).`);
            errorForCallback = error.grpcError;
            errorToThrow = new VortexApiError( 
              `Failed to ${operationName} attempt ${attempts}. Last error: ${errorForCallback.message}`,
              { statusCode: errorForCallback.code, details: errorForCallback.details, grpcError: errorForCallback, isClientTimeout: false } // Ensure isClientTimeout is false here
            );
        } else if (error instanceof VortexApiError) {
            console.log(`[${operationName}] Caught error is VortexApiError without grpcError (and not client timeout).`);
            errorForCallback = { code: error.statusCode || grpc.status.UNKNOWN, message: error.message, details: error.details || error.message, metadata: new grpc.Metadata(), name: 'VortexApiErrorWrapper' } as grpc.ServiceError;
            errorToThrow = new VortexApiError( 
              `Failed to ${operationName} attempt ${attempts}. Last error: ${errorForCallback.message}`,
              { statusCode: errorForCallback.code, details: errorForCallback.details, grpcError: errorForCallback, isClientTimeout: false } // Ensure isClientTimeout is false here
            );
        } else { 
            console.log(`[${operationName}] Caught error is a raw grpc.ServiceError or other error (not client timeout).`);
            errorForCallback = error as grpc.ServiceError;
            errorToThrow = new VortexApiError(
              `Failed to ${operationName} attempt ${attempts}. Last error: ${errorForCallback.message}`,
              { statusCode: errorForCallback.code, details: errorForCallback.details, grpcError: errorForCallback, isClientTimeout: false } // Ensure isClientTimeout is false here
            );
        }
        
        // Use the isClientTimeoutError flag determined at the start of the catch block for the condition
        console.log(`[${operationName}] Processed error. isClientTimeout flag for decision: ${isClientTimeoutError}. Error code from errorForCallback: ${errorForCallback.code}. Attempt: ${attempts}, MaxRetries: ${this.clientOptions.maxRetries}`);

        if (isClientTimeoutError || attempts > this.clientOptions.maxRetries || !this.clientOptions.retryableStatusCodes.includes(errorForCallback.code as grpc.status) ) {
          console.log(`[${operationName}] Not retrying. ClientTimeout: ${isClientTimeoutError}, Attempts (${attempts}) > maxRetries (${this.clientOptions.maxRetries}): ${attempts > this.clientOptions.maxRetries}, Non-retryable status code (${errorForCallback.code}): ${!this.clientOptions.retryableStatusCodes.includes(errorForCallback.code as grpc.status)}.`);
          if (finalCallback) finalCallback(errorForCallback, null);
          
          // If it's a client-side timeout, errorToThrow is already the original client timeout error.
          // Otherwise, if retries exhausted or non-retryable code, potentially re-format errorToThrow.
          if (!isClientTimeoutError && attempts > this.clientOptions.maxRetries) {
            console.log(`[${operationName}] Retries exhausted. Constructing final error message.`);
            if (this.clientOptions.maxRetries === 0 && attempts === 1) { 
                 errorToThrow = new VortexApiError(
                    `Failed to ${operationName} attempt ${attempts}. Last error: ${errorForCallback.message}`,
                    { statusCode: errorForCallback.code, details: errorForCallback.details, grpcError: errorForCallback }
                );
            } else {
                errorToThrow = new VortexApiError(
                  `Failed to ${operationName} after ${attempts} attempts. Last error: ${errorForCallback.message}`,
                  { statusCode: errorForCallback.code, details: errorForCallback.details, grpcError: errorForCallback }
                );
            }
          }
          console.log(`[${operationName}] Throwing final error (non-client-timeout, or retries exhausted):`, errorToThrow);
          throw errorToThrow; 
        }
        
        console.log(`[${operationName}] Retrying. Current backoff: ${currentBackoffMs}ms`);
        const jitterAmount = Math.random() * currentBackoffMs * this.clientOptions.retryJitter;
        const backoffWithJitter = currentBackoffMs + (Math.random() < 0.5 ? -jitterAmount : jitterAmount);
        
        await new Promise(resolveTimer => setTimeout(resolveTimer, Math.max(0, backoffWithJitter)));
        currentBackoffMs = Math.min(this.clientOptions.maxBackoffMs, currentBackoffMs * this.clientOptions.backoffMultiplier);
      }
    }
    // Should be unreachable if maxRetries >= 0, as the loop's catch block will throw or return.
    // Added as a fallback for safety.
    const fallbackError = new VortexApiError(`Failed to ${operationName} after exhausting retries (fallback).`, { statusCode: grpc.status.INTERNAL });
    if (finalCallback) finalCallback({ code: grpc.status.INTERNAL, message: fallbackError.message, details: fallbackError.message, metadata: new grpc.Metadata(), name: 'VortexClientFallbackError' } as grpc.ServiceError, null);
    throw fallbackError;
  }
  
  /**
   * Establishes connections to the gRPC services (Collections and Points).
   * This method is called by the constructor and can be called again to reconnect.
   * It closes existing stubs before creating new ones.
   * @internal
   */
  private connect(): void {
    if (this._collectionsStub) this._collectionsStub.close();
    if (this._pointsStub) this._pointsStub.close();
    const target = `${this.clientOptions.host}:${this.clientOptions.port}`;
    let credentials;
    if (this.clientOptions.secure) {
      credentials = grpc.credentials.createSsl(
        this.clientOptions.rootCerts,
        this.clientOptions.privateKey,
        this.clientOptions.certChain
      );
    } else {
      credentials = grpc.credentials.createInsecure();
    }
    // TODO: If prefix is set, it might need to be passed via grpcClientOptions for certain proxies (e.g., envoy's path rewriting)
    // or handled by a custom interceptor if the gRPC library supports it for path manipulation.
    // For now, prefix is not directly used in channel creation.
    this._collectionsStub = new CollectionsServiceClient(target, credentials, this.clientOptions.grpcClientOptions);
    this._pointsStub = new PointsServiceClient(target, credentials, this.clientOptions.grpcClientOptions);
  }

  /**
   * Closes the gRPC connections to the Vortex server.
   * It's recommended to call this when the client is no longer needed to free up resources.
   */
  public close(): void {
    if (this._collectionsStub) this._collectionsStub.close();
    if (this._pointsStub) this._pointsStub.close();
    this._collectionsStub = null;
    this._pointsStub = null;
  }

  // --- Collections Service Methods ---

  /**
   * Creates a new collection in the Vortex database.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link createCollectionAsync}.
   * @param {string} collectionName - The name of the collection to create.
   * @param {number} vectorDimensions - The dimensionality of the vectors that will be stored in this collection.
   * @param {models.DistanceMetric} distanceMetric - The distance metric to use for similarity search.
   * @param {models.HnswConfigParams | null} [hnswConfig] - Optional HNSW indexing parameters. If null or undefined, server defaults will be used.
   * @param {(err: grpc.ServiceError | null, response: collections_service_pb.CreateCollectionResponse | null) => void} [callback] - Optional callback function.
   *        If not provided, unhandled promise rejections will be logged to console.error.
   */
  public createCollection(
    collectionName: string, vectorDimensions: number, distanceMetric: models.DistanceMetric,
    hnswConfig?: models.HnswConfigParams | null,
    callback?: (err: grpc.ServiceError | null, response: collections_service_pb.CreateCollectionResponse | null) => void
  ): void {
    if (!this._collectionsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      if (callback) callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      else console.error("Unhandled VortexClientError in createCollection (client not connected):", err);
      return;
    }
    const request = new collections_service_pb.CreateCollectionRequest();
    request.setCollectionName(collectionName);
    request.setVectorDimensions(vectorDimensions);
    request.setDistanceMetric(conversions.tsToGrpcDistanceMetric(distanceMetric));
    if (hnswConfig) request.setHnswConfig(conversions.tsToGrpcHnswConfigParams(hnswConfig));
    
    this._execute_with_retry_ts<collections_service_pb.CreateCollectionRequest, collections_service_pb.CreateCollectionResponse, collections_service_pb.CreateCollectionResponse>(
        this._collectionsStub.createCollection.bind(this._collectionsStub), request, "createCollection", callback
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in createCollection:", err); });
  }

  /**
   * Retrieves information about a specific collection.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link getCollectionInfoAsync}.
   * @param {string} collectionName - The name of the collection to retrieve information for.
   * @param {(err: grpc.ServiceError | null, response: models.CollectionInfo | null) => void} callback - Callback function.
   */
  public getCollectionInfo(
    collectionName: string, callback: (err: grpc.ServiceError | null, response: models.CollectionInfo | null) => void
  ): void {
    if (!this._collectionsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      return;
    }
    const request = new collections_service_pb.GetCollectionInfoRequest();
    request.setCollectionName(collectionName);
    this._execute_with_retry_ts<collections_service_pb.GetCollectionInfoRequest, collections_service_pb.GetCollectionInfoResponse, models.CollectionInfo>(
      this._collectionsStub.getCollectionInfo.bind(this._collectionsStub), request, "getCollectionInfo", callback, conversions.grpcToTsCollectionInfo
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in getCollectionInfo:", err); });
  }

  /**
   * Lists all available collections in the Vortex database.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link listCollectionsAsync}.
   * @param {(err: grpc.ServiceError | null, response: models.CollectionDescription[] | null) => void} callback - Callback function.
   */
  public listCollections(
    callback: (err: grpc.ServiceError | null, response: models.CollectionDescription[] | null) => void
  ): void {
    if (!this._collectionsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      return;
    }
    const request = new collections_service_pb.ListCollectionsRequest();
    this._execute_with_retry_ts<collections_service_pb.ListCollectionsRequest, collections_service_pb.ListCollectionsResponse, models.CollectionDescription[]>(
      this._collectionsStub.listCollections.bind(this._collectionsStub), request, "listCollections", callback,
      (grpcResp) => grpcResp.getCollectionsList().map(conversions.grpcToTsCollectionDescription)
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in listCollections:", err); });
  }

  /**
   * Deletes a collection from the Vortex database.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link deleteCollectionAsync}.
   * @param {string} collectionName - The name of the collection to delete.
   * @param {(err: grpc.ServiceError | null, response: collections_service_pb.DeleteCollectionResponse | null) => void} [callback] - Optional callback function.
   */
  public deleteCollection(
    collectionName: string, callback?: (err: grpc.ServiceError | null, response: collections_service_pb.DeleteCollectionResponse | null) => void
  ): void {
    if (!this._collectionsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      if (callback) callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      else console.error("Unhandled VortexClientError in deleteCollection (client not connected):", err);
      return;
    }
    const request = new collections_service_pb.DeleteCollectionRequest();
    request.setCollectionName(collectionName);
    this._execute_with_retry_ts<collections_service_pb.DeleteCollectionRequest, collections_service_pb.DeleteCollectionResponse, collections_service_pb.DeleteCollectionResponse>(
      this._collectionsStub.deleteCollection.bind(this._collectionsStub), request, "deleteCollection", callback
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in deleteCollection:", err); });
  }

  // --- Points Service Methods ---

  /**
   * Upserts (inserts or updates) points into a specified collection.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link upsertPointsAsync}.
   * @param {string} collectionName - The name of the collection.
   * @param {models.PointStruct[]} points - An array of points to upsert.
   * @param {boolean | null | undefined} waitFlush - If true, waits for the operation to be flushed to disk.
   * @param {(err: grpc.ServiceError | null, response: models.PointOperationStatus[] | null) => void} callback - Callback function.
   */
  public upsertPoints(
    collectionName: string, points: models.PointStruct[], waitFlush: boolean | null | undefined,
    callback: (err: grpc.ServiceError | null, response: models.PointOperationStatus[] | null) => void
  ): void {
    if (!this._pointsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      return;
    }
    const request = new points_service_pb.UpsertPointsRequest();
    request.setCollectionName(collectionName);
    request.setPointsList(points.map(conversions.tsToGrpcPointStruct));
    if (waitFlush !== null && waitFlush !== undefined) request.setWaitFlush(waitFlush);

    const wrappedCallback = (err: grpc.ServiceError | null, grpcResponse: points_service_pb.UpsertPointsResponse | null) => {
      if (err) {
        callback(err, null);
      } else if (grpcResponse) {
        if (grpcResponse.getOverallError()) {
          const overallError = new VortexApiError(`Overall error during upsert: ${grpcResponse.getOverallError()}`, { statusCode: grpc.status.UNKNOWN, details: grpcResponse.getOverallError() });
          callback(overallError.grpcError || {code: grpc.status.UNKNOWN, message:overallError.message, details:overallError.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
        } else {
          callback(null, grpcResponse.getStatusesList().map(conversions.grpcToTsPointOperationStatus));
        }
      } else {
        const unknownError = new VortexApiError("Unknown error: No response/error from upsertPoints.", {statusCode: grpc.status.UNKNOWN});
        callback(unknownError.grpcError || {code: grpc.status.UNKNOWN, message:unknownError.message, details:unknownError.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      }
    };
    this._execute_with_retry_ts<points_service_pb.UpsertPointsRequest, points_service_pb.UpsertPointsResponse, points_service_pb.UpsertPointsResponse>( // Note: ConvertedRes is UpsertPointsResponse here
      this._pointsStub.upsertPoints.bind(this._pointsStub), request, "upsertPoints", wrappedCallback // Pass wrappedCallback which handles conversion internally
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in upsertPoints:", err); });
  }

  /**
   * Retrieves points from a collection by their IDs.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link getPointsAsync}.
   * @param {string} collectionName - The name of the collection.
   * @param {string[]} ids - An array of point IDs to retrieve.
   * @param {boolean | null | undefined} withPayload - If true, includes the payload in the response.
   * @param {boolean | null | undefined} withVector - If true, includes the vector in the response.
   * @param {(err: grpc.ServiceError | null, response: models.PointStruct[] | null) => void} callback - Callback function.
   */
  public getPoints(
    collectionName: string, ids: string[], withPayload: boolean | null | undefined, withVector: boolean | null | undefined,
    callback: (err: grpc.ServiceError | null, response: models.PointStruct[] | null) => void
  ): void {
    if (!this._pointsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      return;
    }
    const request = new points_service_pb.GetPointsRequest();
    request.setCollectionName(collectionName);
    request.setIdsList(ids);
    if (withPayload !== null && withPayload !== undefined) request.setWithPayload(withPayload);
    if (withVector !== null && withVector !== undefined) request.setWithVector(withVector);
    
    this._execute_with_retry_ts<points_service_pb.GetPointsRequest, points_service_pb.GetPointsResponse, models.PointStruct[]>(
      this._pointsStub.getPoints.bind(this._pointsStub), request, "getPoints", callback,
      (grpcResp) => grpcResp.getPointsList().map(conversions.grpcToTsPointStruct)
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in getPoints:", err); });
  }

  /**
   * Deletes points from a collection by their IDs.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link deletePointsAsync}.
   * @param {string} collectionName - The name of the collection.
   * @param {string[]} ids - An array of point IDs to delete.
   * @param {boolean | null | undefined} waitFlush - If true, waits for the operation to be flushed to disk.
   * @param {(err: grpc.ServiceError | null, response: models.PointOperationStatus[] | null) => void} callback - Callback function.
   */
  public deletePoints(
    collectionName: string, ids: string[], waitFlush: boolean | null | undefined,
    callback: (err: grpc.ServiceError | null, response: models.PointOperationStatus[] | null) => void
  ): void {
    if (!this._pointsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      return;
    }
    const request = new points_service_pb.DeletePointsRequest();
    request.setCollectionName(collectionName);
    request.setIdsList(ids);
    if (waitFlush !== null && waitFlush !== undefined) request.setWaitFlush(waitFlush);

    const wrappedCallback = (err: grpc.ServiceError | null, grpcResponse: points_service_pb.DeletePointsResponse | null) => {
      if (err) {
        callback(err, null);
      } else if (grpcResponse) {
        if (grpcResponse.getOverallError()) {
          const overallError = new VortexApiError(`Overall error during delete: ${grpcResponse.getOverallError()}`, {statusCode: grpc.status.UNKNOWN, details: grpcResponse.getOverallError()});
          callback(overallError.grpcError || {code: grpc.status.UNKNOWN, message:overallError.message, details:overallError.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
        } else {
          callback(null, grpcResponse.getStatusesList().map(conversions.grpcToTsPointOperationStatus));
        }
      } else {
        const unknownError = new VortexApiError("Unknown error: No response/error from deletePoints.", {statusCode: grpc.status.UNKNOWN});
        callback(unknownError.grpcError || {code: grpc.status.UNKNOWN, message:unknownError.message, details:unknownError.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      }
    };
    this._execute_with_retry_ts<points_service_pb.DeletePointsRequest, points_service_pb.DeletePointsResponse, points_service_pb.DeletePointsResponse>(
      this._pointsStub.deletePoints.bind(this._pointsStub), request, "deletePoints", wrappedCallback
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in deletePoints:", err); });
  }

  /**
   * Searches for points in a collection similar to a query vector.
   * This is a callback-style asynchronous operation. For a Promise-based version, see {@link searchPointsAsync}.
   * @param {string} collectionName - The name of the collection to search in.
   * @param {models.Vector} queryVector - The vector to search with.
   * @param {number} kLimit - The maximum number of results to return.
   * @param {models.Filter | null | undefined} filter - Optional filter conditions.
   * @param {boolean | null | undefined} withPayload - If true, includes the payload in the results.
   * @param {boolean | null | undefined} withVector - If true, includes the vector in the results.
   * @param {models.SearchParams | null | undefined} [searchParams] - Optional search parameters (e.g., HNSW `ef_search`).
   * @param {(err: grpc.ServiceError | null, response: models.ScoredPoint[] | null) => void} [callback] - Optional callback function.
   */
  public searchPoints(
    collectionName: string, queryVector: models.Vector, kLimit: number,
    filter: models.Filter | null | undefined, withPayload: boolean | null | undefined, withVector: boolean | null | undefined,
    searchParams?: models.SearchParams | null | undefined,
    callback?: (err: grpc.ServiceError | null, response: models.ScoredPoint[] | null) => void
  ): void {
    if (!this._pointsStub) {
      const err = new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
      if (callback) callback(err.grpcError || {code: grpc.status.UNAVAILABLE, message:err.message, details:err.message, metadata: new grpc.Metadata(), name:"VortexClientError"} as grpc.ServiceError, null);
      else console.error("Unhandled VortexClientError in searchPoints (client not connected):", err);
      return;
    }
    const request = new points_service_pb.SearchPointsRequest();
    request.setCollectionName(collectionName);
    request.setQueryVector(conversions.tsToGrpcVector(queryVector));
    request.setKLimit(kLimit);
    if (filter) request.setFilter(conversions.tsToGrpcFilter(filter));
    if (withPayload !== null && withPayload !== undefined) request.setWithPayload(withPayload);
    if (withVector !== null && withVector !== undefined) request.setWithVector(withVector);
    if (searchParams) request.setParams(conversions.tsToGrpcSearchParams(searchParams));

    this._execute_with_retry_ts<points_service_pb.SearchPointsRequest, points_service_pb.SearchPointsResponse, models.ScoredPoint[]>(
      this._pointsStub.searchPoints.bind(this._pointsStub), request, "searchPoints", callback, 
      (grpcResp) => grpcResp.getResultsList().map(conversions.grpcToTsScoredPoint)
    ).catch(err => { if (!callback) console.error("Unhandled promise rejection in searchPoints:", err); });
  }

  // --- Promise-based Async Methods ---

  /**
   * Creates a new collection in the Vortex database (Promise-based).
   * @param {string} collectionName - The name of the collection to create.
   * @param {number} vectorDimensions - The dimensionality of the vectors.
   * @param {models.DistanceMetric} distanceMetric - The distance metric for similarity search.
   * @param {models.HnswConfigParams | null} [hnswConfig] - Optional HNSW indexing parameters.
   * @returns {Promise<collections_service_pb.CreateCollectionResponse>} A promise that resolves with the gRPC response.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async createCollectionAsync(
    collectionName: string, vectorDimensions: number, distanceMetric: models.DistanceMetric, hnswConfig?: models.HnswConfigParams | null
  ): Promise<collections_service_pb.CreateCollectionResponse> {
    if (!this._collectionsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new collections_service_pb.CreateCollectionRequest();
    request.setCollectionName(collectionName);
    request.setVectorDimensions(vectorDimensions);
    request.setDistanceMetric(conversions.tsToGrpcDistanceMetric(distanceMetric));
    if (hnswConfig) request.setHnswConfig(conversions.tsToGrpcHnswConfigParams(hnswConfig));
    return this._execute_with_retry_ts(this._collectionsStub.createCollection.bind(this._collectionsStub), request, "createCollectionAsync");
  }

  /**
   * Retrieves information about a specific collection (Promise-based).
   * @param {string} collectionName - The name of the collection.
   * @returns {Promise<models.CollectionInfo>} A promise that resolves with the collection information.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async getCollectionInfoAsync(collectionName: string): Promise<models.CollectionInfo> {
    if (!this._collectionsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new collections_service_pb.GetCollectionInfoRequest();
    request.setCollectionName(collectionName);
    const grpcResponse = await this._execute_with_retry_ts(this._collectionsStub.getCollectionInfo.bind(this._collectionsStub), request, "getCollectionInfoAsync");
    return conversions.grpcToTsCollectionInfo(grpcResponse);
  }

  /**
   * Lists all available collections in the Vortex database (Promise-based).
   * @returns {Promise<models.CollectionDescription[]>} A promise that resolves with an array of collection descriptions.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async listCollectionsAsync(): Promise<models.CollectionDescription[]> {
    if (!this._collectionsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new collections_service_pb.ListCollectionsRequest();
    const grpcResponse = await this._execute_with_retry_ts(this._collectionsStub.listCollections.bind(this._collectionsStub), request, "listCollectionsAsync");
    return grpcResponse.getCollectionsList().map(conversions.grpcToTsCollectionDescription);
  }

  /**
   * Deletes a collection from the Vortex database (Promise-based).
   * @param {string} collectionName - The name of the collection to delete.
   * @returns {Promise<collections_service_pb.DeleteCollectionResponse>} A promise that resolves with the gRPC response.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async deleteCollectionAsync(collectionName: string): Promise<collections_service_pb.DeleteCollectionResponse> {
    if (!this._collectionsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new collections_service_pb.DeleteCollectionRequest();
    request.setCollectionName(collectionName);
    return this._execute_with_retry_ts(this._collectionsStub.deleteCollection.bind(this._collectionsStub), request, "deleteCollectionAsync");
  }

  /**
   * Upserts (inserts or updates) points into a specified collection (Promise-based).
   * @param {string} collectionName - The name of the collection.
   * @param {models.PointStruct[]} points - An array of points to upsert.
   * @param {boolean | null | undefined} waitFlush - If true, waits for the operation to be flushed to disk.
   * @returns {Promise<models.PointOperationStatus[]>} A promise that resolves with an array of operation statuses for each point.
   * @throws {VortexApiError} If the client is not connected or the operation fails (e.g., overall error from server).
   */
  public async upsertPointsAsync(
    collectionName: string, points: models.PointStruct[], waitFlush: boolean | null | undefined
  ): Promise<models.PointOperationStatus[]> {
    if (!this._pointsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new points_service_pb.UpsertPointsRequest();
    request.setCollectionName(collectionName);
    request.setPointsList(points.map(conversions.tsToGrpcPointStruct));
    if (waitFlush !== null && waitFlush !== undefined) request.setWaitFlush(waitFlush);
    const grpcResponse = await this._execute_with_retry_ts(this._pointsStub.upsertPoints.bind(this._pointsStub), request, "upsertPointsAsync");
    if (grpcResponse.getOverallError()) {
        throw new VortexApiError(`Overall error during upsert: ${grpcResponse.getOverallError()}`, { statusCode: grpc.status.UNKNOWN, details: grpcResponse.getOverallError() });
    }
    return grpcResponse.getStatusesList().map(conversions.grpcToTsPointOperationStatus);
  }

  /**
   * Retrieves points from a collection by their IDs (Promise-based).
   * @param {string} collectionName - The name of the collection.
   * @param {string[]} ids - An array of point IDs to retrieve.
   * @param {boolean | null | undefined} withPayload - If true, includes the payload in the response.
   * @param {boolean | null | undefined} withVector - If true, includes the vector in the response.
   * @returns {Promise<models.PointStruct[]>} A promise that resolves with an array of retrieved points.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async getPointsAsync(
    collectionName: string, ids: string[], withPayload: boolean | null | undefined, withVector: boolean | null | undefined
  ): Promise<models.PointStruct[]> {
    if (!this._pointsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new points_service_pb.GetPointsRequest();
    request.setCollectionName(collectionName);
    request.setIdsList(ids);
    if (withPayload !== null && withPayload !== undefined) request.setWithPayload(withPayload);
    if (withVector !== null && withVector !== undefined) request.setWithVector(withVector);
    const grpcResponse = await this._execute_with_retry_ts(this._pointsStub.getPoints.bind(this._pointsStub), request, "getPointsAsync");
    return grpcResponse.getPointsList().map(conversions.grpcToTsPointStruct);
  }

  /**
   * Deletes points from a collection by their IDs (Promise-based).
   * @param {string} collectionName - The name of the collection.
   * @param {string[]} ids - An array of point IDs to delete.
   * @param {boolean | null | undefined} waitFlush - If true, waits for the operation to be flushed to disk.
   * @returns {Promise<models.PointOperationStatus[]>} A promise that resolves with an array of operation statuses for each point.
   * @throws {VortexApiError} If the client is not connected or the operation fails (e.g., overall error from server).
   */
  public async deletePointsAsync(
    collectionName: string, ids: string[], waitFlush: boolean | null | undefined
  ): Promise<models.PointOperationStatus[]> {
    if (!this._pointsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new points_service_pb.DeletePointsRequest();
    request.setCollectionName(collectionName);
    request.setIdsList(ids);
    if (waitFlush !== null && waitFlush !== undefined) request.setWaitFlush(waitFlush);
    const grpcResponse = await this._execute_with_retry_ts(this._pointsStub.deletePoints.bind(this._pointsStub), request, "deletePointsAsync");
    if (grpcResponse.getOverallError()) {
        throw new VortexApiError(`Overall error during delete: ${grpcResponse.getOverallError()}`, { statusCode: grpc.status.UNKNOWN, details: grpcResponse.getOverallError() });
    }
    return grpcResponse.getStatusesList().map(conversions.grpcToTsPointOperationStatus);
  }

  /**
   * Searches for points in a collection similar to a query vector (Promise-based).
   * @param {string} collectionName - The name of the collection to search in.
   * @param {models.Vector} queryVector - The vector to search with.
   * @param {number} kLimit - The maximum number of results to return.
   * @param {models.Filter | null | undefined} filter - Optional filter conditions.
   * @param {boolean | null | undefined} withPayload - If true, includes the payload in the results.
   * @param {boolean | null | undefined} withVector - If true, includes the vector in the results.
   * @param {models.SearchParams | null | undefined} [searchParams] - Optional search parameters (e.g., HNSW `ef_search`).
   * @returns {Promise<models.ScoredPoint[]>} A promise that resolves with an array of scored points.
   * @throws {VortexApiError} If the client is not connected or the operation fails.
   */
  public async searchPointsAsync(
    collectionName: string, queryVector: models.Vector, kLimit: number,
    filter: models.Filter | null | undefined, withPayload: boolean | null | undefined, withVector: boolean | null | undefined,
    searchParams?: models.SearchParams | null | undefined
  ): Promise<models.ScoredPoint[]> {
    if (!this._pointsStub) throw new VortexApiError("Client not connected", {statusCode: grpc.status.UNAVAILABLE});
    const request = new points_service_pb.SearchPointsRequest();
    request.setCollectionName(collectionName);
    request.setQueryVector(conversions.tsToGrpcVector(queryVector));
    request.setKLimit(kLimit);
    if (filter) request.setFilter(conversions.tsToGrpcFilter(filter));
    if (withPayload !== null && withPayload !== undefined) request.setWithPayload(withPayload);
    if (withVector !== null && withVector !== undefined) request.setWithVector(withVector);
    if (searchParams) request.setParams(conversions.tsToGrpcSearchParams(searchParams));
    const grpcResponse = await this._execute_with_retry_ts(this._pointsStub.searchPoints.bind(this._pointsStub), request, "searchPointsAsync");
    return grpcResponse.getResultsList().map(conversions.grpcToTsScoredPoint);
  }
}
