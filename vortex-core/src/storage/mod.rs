// Declare the mmap_vector_storage module
pub mod mmap_vector_storage;
// Declare the mmap_hnsw_graph_links module
pub mod mmap_hnsw_graph_links;

// Potentially re-export items from mmap_vector_storage if needed publicly
// pub use mmap_vector_storage::MmapVectorStorage;
// Potentially re-export items from mmap_hnsw_graph_links if needed publicly
// pub use mmap_hnsw_graph_links::MmapHnswGraphLinks;

#[cfg(test)]
mod mmap_vector_storage_tests;

#[cfg(test)]
mod mmap_hnsw_graph_links_tests;
