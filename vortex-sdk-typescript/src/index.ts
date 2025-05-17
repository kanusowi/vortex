/**
 * @fileoverview This is the main entry point for the Vortex TypeScript SDK.
 * It re-exports all public APIs, including models, the client, and exceptions,
 * making them available for consumers of the SDK package.
 *
 * @example
 * ```typescript
 * import { VortexClient, PointStruct, DistanceMetric } from 'vortex-sdk-typescript';
 *
 * const client = new VortexClient({ host: 'localhost', port: 50051 });
 *
 * async function example() {
 *   await client.createCollection({
 *     collectionName: 'my_collection',
 *     vectorSize: 128,
 *     distanceMetric: DistanceMetric.COSINE,
 *   });
 *
 *   const point: PointStruct = {
 *     id: '1',
 *     vector: { elements: Array(128).fill(0).map(() => Math.random()) },
 *     payload: { fields: { "key": "value" } }
 *   };
 *   await client.upsertPoints({ collectionName: 'my_collection', points: [point] });
 * }
 *
 * example();
 * ```
 */

export * from './models';
export * from './client';
export * from './exceptions';
