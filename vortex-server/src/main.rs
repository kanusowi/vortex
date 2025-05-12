use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::net::SocketAddr;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config; // Basic config for now
mod error;
mod handlers;
mod models;
mod state;

use state::AppState;

#[tokio::main]
async fn main() {
    // Initialize tracing subscriber for logging
    // Use RUST_LOG env var to control level (e.g., RUST_LOG=vortex_server=debug,vortex_core=info)
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("vortex_server=info".parse().unwrap()).add_directive("vortex_core=info".parse().unwrap())) // Default level info
        .init();

    info!("Initializing Vortex Server...");

    // Create shared application state
    // Using the RwLock around the HnswIndex directly for now to simplify mutation
    let app_state = AppState::new();
    info!("Application state created.");

    // Define API routes
    let app = Router::new()
        // Routes for /indices
        .route("/indices", get(handlers::list_indices)) // Added GET for listing indices
        .route("/indices", post(handlers::create_index))
        // Routes for /indices/:name/vectors
        .route("/indices/:name/vectors", get(handlers::list_vectors)) // Existing GET for listing vectors
        // Route for adding/updating a single vector (PUT) - Keep this distinct
        .route("/indices/:name/vectors", put(handlers::add_vector)) 
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
        .with_state(app_state); // Provide shared state to handlers

    // Define server address
    // TODO: Load from config file/env vars
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Starting server on {}", addr);

    // Run the server
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
