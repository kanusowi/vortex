// This module includes the Rust code generated from our .proto files by tonic_build.

// The package name specified in the .proto files is "vortex.api.v1".
// tonic::include_proto! will create a Rust module named "vortex_api_v1" (hyphens/dots replaced by underscores).
// Inside this module, you'll find the generated structs and service traits.

pub mod vortex_api_v1 {
    tonic::include_proto!("vortex.api.v1"); // Matches the package name in .proto files
}

// Optional: Re-export commonly used generated types for convenience.
// For example, if you want to use `CollectionsServiceClient` directly from `crate::grpc_api::CollectionsServiceClient`.
/*
pub use vortex_api_v1::collections_service_client::CollectionsServiceClient;
pub use vortex_api_v1::points_service_client::PointsServiceClient;
pub use vortex_api_v1::{
    // Common messages
    Vector,
    Payload,
    PointStruct,
    ScoredPoint,
    Filter,
    HnswConfigParams,
    DistanceMetric,
    SearchParams,
    PointOperationStatus,
    StatusCode,
    // CollectionsService messages
    CreateCollectionRequest,
    CreateCollectionResponse,
    GetCollectionInfoRequest,
    GetCollectionInfoResponse,
    ListCollectionsRequest,
    ListCollectionsResponse,
    CollectionDescription,
    DeleteCollectionRequest,
    DeleteCollectionResponse,
    CollectionStatus,
    // PointsService messages
    UpsertPointsRequest,
    UpsertPointsResponse,
    GetPointsRequest,
    GetPointsResponse,
    DeletePointsRequest,
    DeletePointsResponse,
    SearchPointsRequest,
    SearchPointsResponse,
};
*/
// We will uncomment and refine re-exports as we implement the services.
