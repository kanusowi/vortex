use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use vortex_core::config::HnswConfig;
use vortex_core::distance::DistanceMetric;
use tempfile::tempdir; // Added for temporary directories
// Removed VortexResult as it's not directly used for assertions here, .unwrap() is used.
use vortex_core::index::{HnswIndex, Index};
use vortex_core::vector::{Embedding, VectorId};
// use vortex_core::utils::create_rng;

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
// ndarray::Array1 is not directly used if Embedding::from(Vec<f32>) is used.
// use ndarray::Array1; 

const DIM: usize = 128; // Default dimensionality for benchmarks

// --- Data Generation Helper Functions ---

fn generate_random_vector(dim: usize, rng: &mut StdRng) -> Embedding {
    let vec: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>() * 2.0 - 1.0).collect(); // Values between -1 and 1
    vec.into()
}

fn generate_vector_id(i: usize) -> VectorId {
    format!("vec_{}", i)
}

fn generate_test_data(num_vectors: usize, dim: usize, seed: u64) -> Vec<(VectorId, Embedding)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..num_vectors)
        .map(|i| (generate_vector_id(i), generate_random_vector(dim, &mut rng)))
        .collect()
}

// --- Benchmark Functions ---

fn bench_build_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_index");
    let dim = DIM;
    let seed = 1u64; // Consistent seed for data generation

    // Realistic HNSW config
    let base_config = HnswConfig { // Removed mut as it's cloned and modified later
        vector_dim: dim as u32, 
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 50,
        ml: 1.0 / (16.0f64.ln()),
        seed: Some(seed),
        vector_storage_capacity: None, 
        graph_links_capacity: None,    
    };

    for n_val in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*n_val as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n_val), n_val, |b, &n| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            // Set capacities based on n for this specific benchmark run
            let mut current_run_config = base_config;
            current_run_config.vector_storage_capacity = Some(n + 100); // Add some buffer
            current_run_config.graph_links_capacity = Some(n + 100);   // Add some buffer

            b.iter_batched(
                || {
                    let data = generate_test_data(n, dim, seed);
                    let dir = tempdir().unwrap(); 
                    (data, dir)
                },
                |(data, dir)| { 
                    let index_path = dir.path();
                    let index_name = "build_index_bench";
                    // Use current_run_config with capacities set
                    let mut index = rt.block_on(HnswIndex::new(index_path, index_name, current_run_config, DistanceMetric::L2)).unwrap();
                    for (id, vector) in data {
                        rt.block_on(index.add_vector(black_box(id), black_box(vector))).unwrap();
                    }
                    (index, dir) 
                },
                criterion::BatchSize::SmallInput, 
            );
        });
    }
    group.finish();
}

fn bench_add_vector_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_vector_single");
    let dim = DIM;
    let seed = 2u64;

    for n_val in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(n_val), n_val, |b, &n| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            
            let mut i = n; // Counter for unique vector IDs

            b.iter_batched(
                || { // Setup for each batch of measurements
                    let dir = tempdir().unwrap(); 
                    let index_path = dir.path().to_path_buf(); // Keep path owned
                    let index_name = "add_single_bench_iter";

                    let current_config = HnswConfig {
                        vector_dim: dim as u32, 
                        m: 16, m_max0: 32, ef_construction: 100, ef_search: 50, 
                        ml: 1.0 / (16.0f64.ln()), seed: Some(seed),
                        // Capacity for n initial vectors + a few more for the benchmarked adds within a sample
                        vector_storage_capacity: Some(n + 10), 
                        graph_links_capacity: Some(n + 10),
                    };

                    let initial_data = generate_test_data(n, dim, seed);
                    let mut index = rt.block_on(HnswIndex::new(&index_path, index_name, current_config, DistanceMetric::L2)).unwrap();
                    for (id, vector) in initial_data {
                        rt.block_on(index.add_vector(id, vector)).unwrap();
                    }
                    
                    i += 1; // Increment for the new vector to be added
                    let new_vector_data = (generate_vector_id(i), generate_random_vector(dim, &mut StdRng::seed_from_u64(seed + i as u64)));
                    (index, new_vector_data, dir) // Pass index, new data, and dir
                },
                |(mut fresh_index, (id, vector), _dir)| { // Routine: add the single vector to the fresh_index
                    rt.block_on(fresh_index.add_vector(black_box(id), black_box(vector))).unwrap();
                },
                criterion::BatchSize::SmallInput, // SmallInput because setup is non-trivial
            );
        });
    }
    group.finish();
}

fn bench_add_vector_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_vector_batch");
    let dim = DIM;
    let initial_seed = 3u64;

    let n_values = [1000, 10000];
    let b_values = [100, 500];

    for n_val in n_values.iter() {
        for b_val in b_values.iter() {
            group.throughput(Throughput::Elements(*b_val as u64)); 
            group.bench_with_input(
                BenchmarkId::new(format!("N={}", n_val), b_val), 
                &(*n_val, *b_val), 
                |b, &(n, batch_size)| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                                        
                    b.iter_batched(
                        || { 
                            let dir = tempdir().unwrap();
                            let index_path = dir.path().to_path_buf(); 
                            let index_name = "add_batch_bench";
                            
                            // Ensure base_config is not used directly if it's missing capacities for this specific setup
                            let run_config = HnswConfig {
                                vector_dim: dim as u32,
                                m: 16, m_max0: 32, ef_construction: 100, ef_search: 50,
                                ml: 1.0 / (16.0f64.ln()), seed: Some(initial_seed),
                                vector_storage_capacity: Some(n + batch_size + 200), // Increased buffer
                                graph_links_capacity: Some(n + batch_size + 200),    // Increased buffer
                            };
                            
                            let mut current_base_index = rt.block_on(HnswIndex::new(&index_path, index_name, run_config, DistanceMetric::L2)).unwrap();
                            let current_initial_data = generate_test_data(n, dim, initial_seed);
                            for (id, vector) in current_initial_data {
                                rt.block_on(current_base_index.add_vector(id, vector)).unwrap();
                            }

                            // current_id_offset logic was flawed for unique IDs across batches, simplify seed for batch data
                            let batch_data_seed = initial_seed + n as u64 + batch_size as u64; // Ensure different seed
                            let batch_vectors = generate_test_data(batch_size, dim, batch_data_seed);
                            (current_base_index, batch_vectors, dir) // Pass dir to keep it alive
                        },
                        |(mut index_for_batch, batch_data, _dir)| { 
                            for (id, vector) in batch_data {
                                rt.block_on(index_for_batch.add_vector(black_box(id), black_box(vector))).unwrap();
                            }
                            index_for_batch 
                        },
                        criterion::BatchSize::SmallInput, 
                    );
                }
            );
        }
    }
    group.finish();
}

fn bench_search_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_latency");
    let dim = DIM;
    let initial_seed = 4u64;

    let n_values = [1000, 10000, 50000];
    let k_values = [1, 10, 50];
    let ef_search_values = [50, 100, 200];

    for n_val_ref in n_values.iter() {
        let n_val = *n_val_ref;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let dir = tempdir().unwrap(); // Temp dir for the index, kept alive for this n_val
        let index_path_for_n = dir.path();
        let index_name_for_n = format!("search_latency_bench_N{}", n_val);

        // Build index once for this n_val
        let current_config_for_n = HnswConfig {
            vector_dim: dim as u32,
            m: 16, m_max0: 32, ef_construction: 100,
            ef_search: ef_search_values.iter().max().copied().unwrap_or(50), // Use a common ef_search for build, will be overridden
            ml: 1.0 / (16.0f64.ln()), seed: Some(initial_seed),
            vector_storage_capacity: Some(n_val + 100),
            graph_links_capacity: Some(n_val + 100),
        };

        let data_for_n = generate_test_data(n_val, dim, initial_seed);
        let mut index_for_n = rt.block_on(HnswIndex::new(index_path_for_n, &index_name_for_n, current_config_for_n, DistanceMetric::L2)).unwrap();
        for (id, vector) in data_for_n {
            rt.block_on(index_for_n.add_vector(id, vector)).unwrap();
        }
        
        // Create a new RNG for query vectors for this specific N value to ensure queries are consistent if benchmarks are re-ordered
        let mut query_rng_for_n = StdRng::seed_from_u64(initial_seed + n_val as u64 + 1);

        for k_val_ref in k_values.iter() {
            let k_val = *k_val_ref;
            for ef_s_val_ref in ef_search_values.iter() {
                let ef_s_val = *ef_s_val_ref;
                if ef_s_val < k_val {
                    continue;
                }

                let bench_id_str = format!("N={}/k={}/ef_search={}", n_val, k_val, ef_s_val);
                group.throughput(Throughput::Elements(1));

                // Generate a new query vector for each specific (k, ef_search) combination to ensure
                // that if Criterion runs samples out of order, each gets a fresh, consistent query.
                let query_vector = generate_random_vector(dim, &mut query_rng_for_n);

                group.bench_with_input(
                    BenchmarkId::from_parameter(bench_id_str),
                    &(k_val, ef_s_val), // Pass only k and ef_search as params to routine
                    |b, &(k_param, ef_param)| {
                        // Index is pre-built (index_for_n)
                        // Runtime is pre-created (rt)
                        b.iter_batched(
                            || query_vector.clone(), // Clone the query vector for this iteration
                            |q_vec| {
                                black_box(rt.block_on(index_for_n.search_with_ef(black_box(q_vec), black_box(k_param), black_box(ef_param))).unwrap());
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    }
                );
            }
        }
        // dir is dropped here, after all (k, ef_s) for this n_val are done
    }
    group.finish();
}

fn bench_multi_segment_search_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_segment_search_latency");
    let dim = DIM;
    let initial_seed = 5u64; // Different seed

    let total_n_values = [10000, 50000]; // Total vectors in the index
    let num_segments_values = [2, 5];    // Number of segments to create
    let k_val = 10;                       // Fixed k for this benchmark
    let ef_s_val = 100;                   // Fixed ef_search

    for total_n_ref in total_n_values.iter() {
        let total_n = *total_n_ref;
        for num_segments_ref in num_segments_values.iter() {
            let num_segments = *num_segments_ref;

            if num_segments == 0 { continue; }

            let n_per_segment = total_n / num_segments;
            if n_per_segment == 0 { continue; }

            let rt = tokio::runtime::Runtime::new().unwrap();
            let dir = tempdir().unwrap(); // Temp dir kept alive for this (total_n, num_segments) combination
            let index_path_for_config = dir.path();
            let index_name_for_config = format!("multi_search_bench_N{}_Seg{}", total_n, num_segments);

            // Build multi-segment index once for this (total_n, num_segments) combination
            let current_config_for_build = HnswConfig {
                vector_dim: dim as u32,
                m: 16, m_max0: 32, ef_construction: 100,
                ef_search: ef_s_val, // ef_s_val is fixed for this benchmark group
                ml: 1.0 / (16.0f64.ln()), seed: Some(initial_seed),
                vector_storage_capacity: Some(total_n + 100 * num_segments),
                graph_links_capacity: Some(total_n + 100 * num_segments),
            };

            let mut index_for_config = rt.block_on(HnswIndex::new(index_path_for_config, &index_name_for_config, current_config_for_build, DistanceMetric::L2)).unwrap();

            let data_seg0 = generate_test_data(n_per_segment, dim, initial_seed);
            for (id, vector) in data_seg0 {
                rt.block_on(index_for_config.add_vector(id, vector)).unwrap();
            }

            for i in 1..num_segments {
                rt.block_on(index_for_config.add_new_segment_for_testing()).unwrap();
                let segment_data_seed = initial_seed + (i * n_per_segment) as u64;
                let segment_data = generate_test_data(n_per_segment, dim, segment_data_seed);
                for (id, vector) in segment_data {
                    rt.block_on(index_for_config.add_vector(id, vector)).unwrap();
                }
            }

            let mut query_rng_for_config = StdRng::seed_from_u64(initial_seed + total_n as u64 + 1);
            let query_vector = generate_random_vector(dim, &mut query_rng_for_config);

            let bench_id_str = format!("TotalN={}/NumSeg={}", total_n, num_segments);
            group.throughput(Throughput::Elements(1));

            group.bench_with_input(
                BenchmarkId::from_parameter(bench_id_str),
                // Pass dummy params to satisfy bench_with_input, actual values are captured
                // k_val and ef_s_val are fixed for this benchmark group
                &(*total_n_ref, *num_segments_ref), 
                |b, _| { // Parameters _total_n_check, _num_seg_check are not used from input tuple
                    // Index is pre-built (index_for_config)
                    // Runtime is pre-created (rt)
                    b.iter_batched(
                        || query_vector.clone(), // Clone the query vector for this iteration
                        |q_vec| {
                            black_box(rt.block_on(index_for_config.search_with_ef(black_box(q_vec), black_box(k_val), black_box(ef_s_val))).unwrap());
                        },
                        criterion::BatchSize::SmallInput,
                    );
                }
            );
            // dir is dropped here, after the benchmark for this (total_n, num_segments) is done
        }
    }
    group.finish();
}


// --- Main Benchmark Registration ---

criterion_group!(benches, bench_build_index, bench_add_vector_single, bench_add_vector_batch, bench_search_latency, bench_multi_segment_search_latency);
criterion_main!(benches);
