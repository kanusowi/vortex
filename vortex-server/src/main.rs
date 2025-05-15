use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::env;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod config; // Basic config for now
mod error;
mod handlers;
mod models;
mod state;
mod persistence; // Added persistence module

use state::AppState;

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
    let app_state = AppState::new(persistence_path.clone()); // Pass persistence_path to AppState::new
    info!("Application state created.");

    // Load indices from disk
    persistence::load_all_indices_on_startup(&app_state, &persistence_path).await;

    // Define API routes
    let app_state_clone_for_shutdown = app_state.clone(); // Clone for shutdown handler
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
        .with_state(app_state); // Provide shared state to handlers

    // Define server address
    // TODO: Load from config file/env vars
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("Starting server on {}", addr);

    // Run the server
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    
    info!("Starting server on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(app_state_clone_for_shutdown, persistence_path_clone_for_shutdown))
        .await
        .unwrap();
}

async fn shutdown_signal(app_state: AppState, persistence_path: PathBuf) {
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
    persistence::save_all_indices(&app_state, &persistence_path).await;
    info!("All indices saved. Shutting down.");
}
