// This file is intentionally left blank.
// The Node and ArcNode structs previously defined here were part of an in-memory HNSW implementation
// and have been replaced by direct operations on memory-mapped files (MmapVectorStorage and MmapHnswGraphLinks)
// as part of the refactoring to support mmap-based storage.
// See vortex-core/src/index.rs for the HnswIndex implementation using mmap.
