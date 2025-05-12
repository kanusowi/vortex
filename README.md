<div align="center">

## High-Performance Vector Similarity Search in Rust 🦀

</div>

## 📋 Overview

Vortex is a high-performance Approximate Nearest Neighbor (ANN) search library and server written entirely in Rust. Built on the HNSW (Hierarchical Navigable Small World) algorithm, Vortex delivers fast vector similarity searches with memory efficiency.

**Perfect for:**
- Retrieval-Augmented Generation (RAG)
- Semantic search
- Recommendation systems
- Any application requiring fast vector similarity comparisons

## ✨ Key Features

- **HNSW Algorithm** - Fast approximate nearest neighbor search
- **Multiple Distance Metrics** - Cosine similarity and Euclidean (L2) distance
- **Pure Rust Implementation** - High performance, memory safety, and cross-platform compatibility
- **Async API Server** - Non-blocking I/O with Axum and Tokio
- **Persistence** - Save and load index state to/from disk
- **Configurable** - Customizable HNSW parameters
- **Embeddable Core** - Use `vortex-core` directly in your Rust applications

## ⚠️ Project Status: Alpha

Vortex is currently in **alpha stage**. Core functionality is implemented, but further testing and optimization are needed before production use.

**Current limitations:**
- One-way connections during insertion
- Soft deletion (nodes marked as deleted but remain in graph)
- Server configuration is mostly hardcoded
- Advanced features like filtering, batch operations, and memory mapping are not yet implemented

## 🏛️ Architecture

The project consists of two main components:

- **`vortex-core`**: The core library with HNSW implementation and minimal dependencies
- **`vortex-server`**: A RESTful API server built with Axum that exposes `vortex-core` functionality

## 🚀 Quick Start

### Prerequisites

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Installation

```bash
# Clone the repository
git clone git@github.com:kanusowi/vortex.git
cd vortex

# Build in release mode
cargo build --release
```

### Running the Server

```bash
# Run with default logging (info level)
RUST_LOG=info cargo run --release -p vortex-server
```

The server starts on `127.0.0.1:3000` by default.

## 🔌 API Usage

### Create an Index

```bash
curl -X POST http://127.0.0.1:3000/indices \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_index",
    "config": {
      "m": 16,
      "m_max0": 32,
      "ef_construction": 200,
      "ef_search": 50,
      "ml": 0.318
    },
    "metric": "Cosine",
    "dimensions": 768
  }'
```

### Add a Vector

```bash
curl -X PUT http://127.0.0.1:3000/indices/my_index/vectors \
  -H "Content-Type: application/json" \
  -d '{
    "id": "vector_id_1",
    "vector": [0.1, 0.2, ..., 0.9]
  }'
```

### Search for Similar Vectors

```bash
curl -X POST http://127.0.0.1:3000/indices/my_index/search \
  -H "Content-Type: application/json" \
  -d '{
    "query_vector": [0.11, 0.21, ..., 0.91],
    "k": 5
  }'
```

### More Endpoints

- **Get Index Stats**: `GET /indices/{name}/stats`
- **Get Vector**: `GET /indices/{name}/vectors/{vector_id}`
- **Delete Vector**: `DELETE /indices/{name}/vectors/{vector_id}`

## 📦 Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
vortex-core = { path = "../vortex-core" }
tokio = { version = "1", features = ["full"] }
```

Example code:

```rust
use vortex_core::{HnswIndex, Index, HnswConfig, DistanceMetric, Embedding, VortexResult};

#[tokio::main]
async fn main() -> VortexResult<()> {
    // Create a new index configuration
    let config = HnswConfig::new(16, 200, 50, 1.0 / (16.0f64.ln()));
    let dimensions = 4;

    // Create index instance
    let mut index = HnswIndex::new(config, DistanceMetric::L2, dimensions)?;

    // Add vectors
    let vec1: Embedding = vec![1.0, 1.0, 0.0, 0.0].into();
    index.add_vector("vec1".to_string(), vec1).await?;

    // Search
    let query: Embedding = vec![1.05, 1.05, 0.05, 0.0].into();
    let results = index.search(query, 2).await?;

    Ok(())
}
```

## ⚙️ Configuration

Control logging with the `RUST_LOG` environment variable:

```bash
# Set server to debug, core to trace
RUST_LOG=vortex_server=debug,vortex_core=trace cargo run --release -p vortex-server
```

## ✅ Testing

```bash
# Run all tests
cargo test --all
```

## 🤝 Contributing

Contributions are welcome! Please feel free to:

- Open issues for bugs or feature requests
- Submit pull requests
- Improve documentation
- Share your experience using Vortex
