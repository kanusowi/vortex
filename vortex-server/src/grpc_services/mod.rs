// This module contains the gRPC service implementations.

pub mod collections_service;
pub mod points_service;
pub mod snapshots_service;

// Re-export the service implementation structs for easier access from main.rs
pub use collections_service::CollectionsServerImpl;
pub use points_service::PointsServerImpl;
pub use snapshots_service::SnapshotsServerImpl;
