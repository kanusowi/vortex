use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::env;
use std::sync::Arc; // Added Arc
use tokio::sync::RwLock; // Added RwLock
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::EnvFilter;

// Use modules from the vortex_server library
use vortex_server::handlers;
use vortex_server::state::AppState;
use vortex_server::persistence;
use vortex_server::grpc_api::vortex_api_v1::{
    collections_service_server::CollectionsServiceServer,
    points_service_server::PointsServiceServer,
};
use vortex_server::grpc_services::{CollectionsServerImpl, PointsServerImpl};
use tonic::transport::Server as TonicServer;
// Note: The original `use state::AppState;` is now covered by `use vortex_server::state::AppState;`

#[cfg(test)]
mod wal_integration_tests; // Added for in-crate integration tests

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vortex_server=info".parse().unwrap()).add_directive("vortex_core=info".parse().unwrap()))
        .init();

    info!("Initializing Vortex Server...");

    // Determine persistence path
    let persistence_path_str = env::var("VORTEX_DATA_PATH").unwrap_or_else(|_| "./vortex_data".to_string());
    let persistence_path = PathBuf::from(persistence_path_str);
    info!("Using persistence path: {:?}", persistence_path);

    // Create shared application state
    let app_state_instance = AppState::new(persistence_path.clone());
    let app_state_arc = Arc::new(RwLock::new(app_state_instance));
    info!("Application state created and wrapped in Arc<RwLock<>>.");

    // Load indices from disk
    // load_all_indices_on_startup expects &AppState. We need to pass a reference to the AppState inside the RwLock.
    {
        let app_state_guard = app_state_arc.read().await;
        persistence::load_all_indices_on_startup(&*app_state_guard, &persistence_path).await;
    }
    

    // Define API routes
    let app_state_clone_for_router = app_state_arc.clone();
    let app_state_clone_for_shutdown = app_state_arc.clone(); 
    let persistence_path_clone_for_shutdown = persistence_path.clone();

    let app = Router::new()
        // Routes for /indices
        .route("/indices", get(handlers::list_indices)) // Added GET for listing indices
        .route("/indices", post(handlers::create_index))
        // Routes for /indices/:name/vectors
        .route("/indices/:name/vectors", get(handlers::list_vectors)) // Existing GET for listing vectors
        // Route for adding/updating a single vector (PUT) - Keep this distinct
        .route("/indices/:name/vectors", put(handlers::add_vector))
        .route("/indices/:name/vectors/batch", post(handlers::batch_add_vectors)) // New route for batch add
        .route("/indices/:name/search", post(handlers::search_vectors))
        .route("/indices/:name/stats", get(handlers::get_index_stats))
        .route(
            "/indices/:name/vectors/:vector_id",
            get(handlers::get_vector),
        )
        .route(
            "/indices/:name/vectors/:vector_id",
            delete(handlers::delete_vector),
        )
        // Add middleware
        .layer(TraceLayer::new_for_http()) // Log requests/responses
        .layer(CorsLayer::permissive()) // Allow all origins (adjust for production)
        .with_state(app_state_clone_for_router); // Provide shared state to handlers

    // Define REST server address
    let rest_addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Starting REST server on {}", rest_addr);

    // Define gRPC server address
    let grpc_addr = SocketAddr::from(([127, 0, 0, 1], 50051));
    info!("Starting gRPC server on {}", grpc_addr);

    // Clone AppState for gRPC services
    let app_state_for_grpc = app_state_arc.clone();

    // Create gRPC service implementations
    let collections_service = CollectionsServerImpl { app_state: app_state_for_grpc.clone() };
    let points_service = PointsServerImpl { app_state: app_state_for_grpc };

    // Spawn gRPC server in a separate Tokio task
    let grpc_server_handle = tokio::spawn(async move {
        TonicServer::builder()
            .add_service(CollectionsServiceServer::new(collections_service))
            .add_service(PointsServiceServer::new(points_service))
            .serve(grpc_addr)
            .await
            .expect("Failed to start gRPC server");
    });
    info!("gRPC server task spawned.");

    // Run the Axum REST server
    let axum_listener = tokio::net::TcpListener::bind(rest_addr).await.unwrap();
    info!("Axum REST server listening on {}", rest_addr);
    axum::serve(axum_listener, app)
        .with_graceful_shutdown(shutdown_signal(app_state_clone_for_shutdown, persistence_path_clone_for_shutdown, grpc_server_handle))
        .await
        .unwrap();
}

async fn shutdown_signal(
    app_state_arc: Arc<RwLock<AppState>>, 
    persistence_path: PathBuf,
    grpc_server_handle: tokio::task::JoinHandle<()>, // Add JoinHandle for gRPC server
) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>(); // On non-Unix, just wait for Ctrl+C

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating graceful shutdown...");
        },
        _ = terminate => {
            info!("Received terminate signal, initiating graceful shutdown...");
        },
    }

    info!("Saving all indices before shutdown...");
    // save_all_indices expects &AppState. We need to pass a reference to the AppState inside the RwLock.
    {
        let app_state_guard = app_state_arc.read().await;
        persistence::save_all_indices(&*app_state_guard, &persistence_path).await;
    }
    info!("All indices saved.");
    
    // Gracefully shutdown gRPC server (optional, as it's in a spawned task that will end with main)
    // For a more controlled shutdown, you might use a channel to signal the gRPC server to stop.
    // Here, we'll just log that we're proceeding to shutdown.
    info!("Proceeding to shutdown gRPC server task...");
    grpc_server_handle.abort(); // Abort the gRPC server task
    info!("gRPC server task signaled for shutdown.");

    info!("Vortex Server shutting down completely.");
}
