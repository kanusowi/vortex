# Vortex: High-Performance Vector Similarity Search in Rust 🦀

Vortex is a high-performance Approximate Nearest Neighbor (ANN) search library and server written entirely in Rust. It implements the HNSW (Hierarchical Navigable Small World) algorithm for fast vector similarity searches, optimized for speed and memory efficiency.

The goal of Vortex is to provide a production-ready, embeddable core library (`vortex-core`) and a minimal, functional API server (`vortex-server`) that leverages Rust's performance characteristics for demanding vector search tasks like Retrieval-Augmented Generation (RAG), semantic search, and recommendation systems.

## ✨ Key Features (Current Stage)

*   **HNSW Algorithm:** Core implementation of the HNSW algorithm for ANN search.
*   **Distance Metrics:** Supports common metrics:
    *   Cosine Similarity
    *   Euclidean (L2) Distance
*   **Pure Rust:** Written entirely in stable Rust for performance, safety, and cross-platform compatibility.
*   **Async API Server:** Minimal RESTful API server built with Axum and Tokio for non-blocking I/O.
*   **Basic Persistence:** Ability to save and load index state to/from disk (`bincode` format).
*   **Configurable:** HNSW parameters (M, ef_construction, ef_search, ml) can be configured per index.
*   **Embeddable Core:** The `vortex-core` library can be used directly within other Rust applications.
*   **Basic Operations:** Create indices, add/update vectors, search vectors, retrieve vectors, delete vectors (soft delete), get index stats.

## ⚠️ Project Status: Alpha / Foundational

Vortex is currently in an early **alpha / foundational stage**. The core HNSW logic and basic API are implemented, but it requires further testing, optimization, and feature development before being considered production-ready.

**Known Limitations / Areas for Improvement:**

*   **HNSW Insertion:** The current implementation performs one-way connections (new node -> neighbors). Full bi-directional connection updates and pruning require further refinement, potentially involving interior mutability or state management redesign.
*   **HNSW Deletion:** Deletion is currently "soft" - nodes are marked as deleted but remain in the graph structure. Search correctly skips deleted nodes, but this doesn't reclaim memory or fully remove the node from traversals initiated at other points. True deletion in HNSW is complex.
*   **Server State Mutability:** The current server architecture uses `Arc<RwLock<HnswIndex>>` for state, which works but could be refined further, especially if supporting multiple index types via `dyn Index` becomes a priority again.
*   **Error Handling:** Error handling is functional but could be more granular in places.
*   **Performance:** Benchmarking and targeted performance optimizations have not yet been performed.
*   **Configuration:** Server configuration (port, etc.) is hardcoded; index persistence paths are handled by the client.
*   **Advanced Features:** Features like filtering during search, batch operations, memory mapping (`mmap`), payload storage/retrieval, and advanced configuration options are not yet implemented.

## 🏛️ Architecture Overview

Vortex uses a Rust workspace with two main crates:

1.  **`vortex-core`:** The core library containing the HNSW implementation, distance metrics, data structures, configuration, error handling, and the main `Index` trait. It has minimal dependencies and is designed to be embeddable.
2.  **`vortex-server`:** A web server built using the Axum framework. It exposes the functionality of `vortex-core` via a RESTful JSON API. It depends on `vortex-core`.

## 🚀 Getting Started

### Prerequisites

*   **Rust:** Ensure you have a recent stable Rust toolchain installed. You can get it from [rustup.rs](https://rustup.rs/).
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    rustc --version
    cargo --version
    ```

### Building the Project

Clone the repository and build the entire workspace using Cargo:

```bash
git clone git@github.com:kanusowi/vortex.git
cd vortex
cargo build --release
```

This command compiles both `vortex-core` and `vortex-server` in release mode with optimizations. The executables and libraries will be located in the `target/release` directory.

### Running the API Server

You can run the API server directly using Cargo:

```bash
# Run in release mode for better performance
cargo run --release -p vortex-server
```

By default, the server will start on `127.0.0.1:3000`.

**Logging:**

The server uses the `tracing` library for logging. You can control the log level using the `RUST_LOG` environment variable.

```bash
# Example: Set default level to info
export RUST_LOG=info
cargo run --release -p vortex-server

# Example: Set server to debug, core to trace
export RUST_LOG=vortex_server=debug,vortex_core=trace
cargo run --release -p vortex-server

# Example: Run with info level directly
RUST_LOG=info cargo run --release -p vortex-server
```

## 🔌 Using the API (`vortex-server`)

You can interact with the running server using tools like `curl` or any HTTP client.

**(Note:** Replace `768` with your desired vector dimensionality in the examples below.)

**1. Create a New Index**

*   **Endpoint:** `POST /indices`
*   **Body (JSON):**
    ```json
    {
      "name": "my_index",
      "config": {
        "m": 16,
        "m_max0": 32,
        "ef_construction": 200,
        "ef_search": 50,
        "ml": 0.318, // Example: Approx 1 / ln(16)
        "seed": null    // Optional: null or integer for deterministic builds
      },
      "metric": "Cosine", // Or "L2"
      "dimensions": 768
    }
    ```
*   **Example (`curl`):**
    ```bash
    curl -X POST http://127.0.0.1:3000/indices \
         -H "Content-Type: application/json" \
         -d '{
              "name": "my_index",
              "config": {"m": 16, "m_max0": 32, "ef_construction": 200, "ef_search": 50, "ml": 0.318, "seed": null},
              "metric": "Cosine",
              "dimensions": 768
            }'
    ```
*   **Success Response (201 Created):**
    ```json
    {
      "message": "Index 'my_index' created successfully"
    }
    ```

**2. Add or Update a Vector**

*   **Endpoint:** `PUT /indices/{name}/vectors`
*   **Path Parameter:** `name` - The name of the index.
*   **Body (JSON):**
    ```json
    {
      "id": "vector_id_1",
      "vector": [0.1, 0.2, ..., 0.9] // Array of f32 with 'dimensions' elements
    }
    ```
*   **Example (`curl`):**
    ```bash
    # Replace [...] with your actual 768-dim vector
    curl -X PUT http://127.0.0.1:3000/indices/my_index/vectors \
         -H "Content-Type: application/json" \
         -d '{
              "id": "vector_id_1",
              "vector": [0.1, 0.2, ..., 0.9]
            }'
    ```
*   **Success Response:**
    *   `201 Created` if the vector was added.
    *   `200 OK` if the vector was updated.
    *   Body: `{"message": "Vector 'vector_id_1' processed successfully"}`

**3. Search for Vectors**

*   **Endpoint:** `POST /indices/{name}/search`
*   **Path Parameter:** `name` - The name of the index.
*   **Body (JSON):**
    ```json
    {
      "query_vector": [0.11, 0.21, ..., 0.91], // Query vector (f32 array)
      "k": 5                                  // Number of neighbors to retrieve
    }
    ```
*   **Example (`curl`):**
    ```bash
    # Replace [...] with your query vector
    curl -X POST http://127.0.0.1:3000/indices/my_index/search \
         -H "Content-Type: application/json" \
         -d '{
              "query_vector": [0.11, 0.21, ..., 0.91],
              "k": 5
            }'
    ```
*   **Success Response (200 OK):**
    ```json
    {
      "results": [
        {"id": "vector_id_3", "score": 0.987}, // Highest similarity (Cosine) or lowest distance (L2)
        {"id": "vector_id_1", "score": 0.954},
        // ... up to k results
      ]
    }
    ```

**4. Get Index Statistics**

*   **Endpoint:** `GET /indices/{name}/stats`
*   **Path Parameter:** `name` - The name of the index.
*   **Example (`curl`):**
    ```bash
    curl http://127.0.0.1:3000/indices/my_index/stats
    ```
*   **Success Response (200 OK):**
    ```json
    {
      "vector_count": 1500,
      "dimensions": 768,
      "config": {
        "m": 16,
        "m_max0": 32,
        "ef_construction": 200,
        "ef_search": 50,
        "ml": 0.318,
        "seed": null
      },
      "metric": "Cosine"
    }
    ```

**5. Get a Vector by ID**

*   **Endpoint:** `GET /indices/{name}/vectors/{vector_id}`
*   **Path Parameters:** `name`, `vector_id`.
*   **Example (`curl`):**
    ```bash
    curl http://127.0.0.1:3000/indices/my_index/vectors/vector_id_1
    ```
*   **Success Response (200 OK):**
    ```json
    {
      "id": "vector_id_1",
      "vector": [0.1, 0.2, ..., 0.9]
    }
    ```
*   **Not Found Response (404 Not Found):**
    ```json
    {
      "error": "Vector ID 'vector_id_unknown' not found"
    }
    ```

**6. Delete a Vector by ID**

*   **Endpoint:** `DELETE /indices/{name}/vectors/{vector_id}`
*   **Path Parameters:** `name`, `vector_id`.
*   **Example (`curl`):**
    ```bash
    curl -X DELETE http://127.0.0.1:3000/indices/my_index/vectors/vector_id_1
    ```
*   **Success Response:** `204 No Content` (with an empty body).
*   **Not Found Response:** `404 Not Found` (with JSON error body).

## 📦 Using the Core Library (`vortex-core`)

You can embed `vortex-core` directly into your Rust applications.

1.  Add `vortex-core` to your `Cargo.toml`:
    ```toml
    [dependencies]
    vortex-core = { path = "../vortex-core" } # Or use version = "0.1.0" if published
    tokio = { version = "1", features = ["full"] } # Needed for async runtime
    ```

2.  Use the library in your code:

    ```rust
    use vortex_core::{HnswIndex, Index, HnswConfig, DistanceMetric, Embedding, VortexResult};
    use std::path::Path;
    use std::fs::File;
    use std::io::{BufReader, BufWriter};

    #[tokio::main]
    async fn main() -> VortexResult<()> {
        // 1. Create a new index configuration
        let config = HnswConfig::new(16, 200, 50, 1.0 / (16.0f64.ln())); // M, ef_construction, ef_search, ml
        let dimensions = 4; // Example dimension

        // 2. Create a new index instance
        let mut index = HnswIndex::new(config, DistanceMetric::L2, dimensions)?;

        // 3. Add some vectors
        let vec1: Embedding = vec![1.0, 1.0, 0.0, 0.0].into();
        let vec2: Embedding = vec![1.1, 1.0, 0.1, 0.0].into(); // Close to vec1
        let vec3: Embedding = vec![5.0, 5.0, 5.0, 5.0].into(); // Far away

        index.add_vector("vec1".to_string(), vec1).await?;
        index.add_vector("vec2".to_string(), vec2).await?;
        index.add_vector("vec3".to_string(), vec3).await?;

        println!("Index size: {}", index.len());

        // 4. Search for nearest neighbors
        let query: Embedding = vec![1.05, 1.05, 0.05, 0.0].into(); // Query near vec1/vec2
        let k = 2;
        let results = index.search(query, k).await?;

        println!("Search Results (k={}):", k);
        for (id, score) in results {
            println!("  ID: {}, Score (Distance): {:.4}", id, score);
        }
        // Expected: vec1 and vec2 should be the top results with low distance scores.

        // 5. Get a specific vector
        if let Some(v) = index.get_vector(&"vec1".to_string()).await? {
            println!("Retrieved vec1: {:?}", v.0.as_slice().unwrap_or_default());
        }

        // 6. Save the index
        let save_path = Path::new("my_hnsw_index.bin");
        { // Scope for writer
             let file = File::create(save_path)
                .map_err(|e| vortex_core::VortexError::IoError { path: save_path.to_path_buf(), source: e })?;
             let mut writer = BufWriter::new(file);
             index.save(&mut writer).await?;
             println!("Index saved to {:?}", save_path);
        } // Writer flushed and file closed here


        // 7. Load the index
        let loaded_index = HnswIndex::load_from_path(save_path, dimensions)?;
        println!("Index loaded successfully. Size: {}", loaded_index.len());

        // Verify search on loaded index
        let query_loaded: Embedding = vec![5.1, 5.0, 4.9, 5.0].into(); // Query near vec3
        let results_loaded = loaded_index.search(query_loaded, 1).await?;
        println!("Search Results (Loaded Index, k=1):");
         for (id, score) in results_loaded {
            println!("  ID: {}, Score (Distance): {:.4}", id, score);
            assert_eq!(id, "vec3"); // Should find vec3
        }

        Ok(())
    }
    ```

## ✅ Running Tests

To run the unit and integration tests for both crates:

```bash
cargo test --all
```

This command will execute all tests defined within `vortex-core` and `vortex-server`.

## ⚙️ Configuration

*   **Server Logging:** Controlled via the `RUST_LOG` environment variable (see [Running the API Server](#running-the-api-server)).
*   **HNSW Parameters:** Configured per-index via the `POST /indices` API endpoint (`m`, `ef_construction`, `ef_search`, `ml`, `seed`).
*   **Persistence:** Index saving/loading is currently triggered manually via API calls or direct library usage. File paths need to be managed by the client application or future server configuration.

## 🤝 Contributing

Contributions are welcome! Please feel free to open an issue to discuss bugs or feature requests, or submit a pull request.
