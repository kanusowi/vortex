use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId}; // Removed Bencher
use vortex_core::config::HnswConfig;
use vortex_core::distance::DistanceMetric;
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
    let hnsw_config = HnswConfig {
        m: 16,
        m_max0: 32,
        ef_construction: 200, // A common value, might be parameterized later
        ef_search: 50,      // Not directly used in build, but part of config
        ml: 1.0 / (16.0f64.ln()),
        seed: Some(seed),
    };

    for n_val in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*n_val as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n_val), n_val, |b, &n| {
            // Setup: Generate data outside the timed loop for iter_batched
            // For iter, setup is part of the loop, but for build_index, the whole process is the routine.
            // So, we use iter, and data generation is part of what's measured, which is fine for "build"
            // Alternatively, for more precise "add" timing within build, iter_batched for each add would be needed,
            // but the request is to benchmark "time to create a new HnswIndex and add N vectors".
            
            // Data generation needs to be fast or done in iter_batched setup.
            // Let's use iter_batched to separate data generation from index creation + population.
            b.iter_batched(
                || generate_test_data(n, dim, seed), // Setup: generate N vectors
                |data| { // Routine: create index and add vectors
                    let mut index = HnswIndex::new(hnsw_config, DistanceMetric::L2, dim).unwrap();
                    for (id, vector) in data {
                        // Use tokio runtime to block on async add_vector
                        // This requires tokio to be a direct dev-dependency or accessible.
                        // For simplicity in a library benchmark, if HnswIndex::add_vector can be made sync for bench, it's easier.
                        // Assuming we need to run it as is (async):
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(index.add_vector(black_box(id), black_box(vector))).unwrap();
                    }
                    index // Return the index to ensure it's not optimized away
                },
                criterion::BatchSize::SmallInput, // Setup is proportional to N, might need adjustment for large N
            );
        });
    }
    group.finish();
}

fn bench_add_vector_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_vector_single");
    let dim = DIM;
    let seed = 2u64; // Different seed for different data

    let hnsw_config = HnswConfig {
        m: 16, m_max0: 32, ef_construction: 100, ef_search: 50, 
        ml: 1.0 / (16.0f64.ln()), seed: Some(seed)
    };

    for n_val in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(1)); // Benchmarking one add operation
        group.bench_with_input(BenchmarkId::from_parameter(n_val), n_val, |b, &n| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let initial_data = generate_test_data(n, dim, seed);
            let mut index = HnswIndex::new(hnsw_config, DistanceMetric::L2, dim).unwrap();
            for (id, vector) in initial_data {
                rt.block_on(index.add_vector(id, vector)).unwrap();
            }
            
            // Data for the vector to be added in the benchmark loop
            let mut i = n; // Ensure unique IDs for new vectors
            b.iter_batched(
                || { // Setup for each iteration: generate one new vector
                    i += 1;
                    (generate_vector_id(i), generate_random_vector(dim, &mut StdRng::seed_from_u64(seed + i as u64)))
                },
                |(id, vector)| { // Routine: add the single vector
                    rt.block_on(index.add_vector(black_box(id), black_box(vector))).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_add_vector_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_vector_batch");
    let dim = DIM;
    let initial_seed = 3u64;

    let hnsw_config = HnswConfig {
        m: 16, m_max0: 32, ef_construction: 100, ef_search: 50,
        ml: 1.0 / (16.0f64.ln()), seed: Some(initial_seed)
    };

    let n_values = [1000, 10000];
    let b_values = [100, 500];

    for n_val in n_values.iter() {
        for b_val in b_values.iter() {
            group.throughput(Throughput::Elements(*b_val as u64)); // Throughput is number of vectors added in batch
            group.bench_with_input(
                BenchmarkId::new(format!("N={}", n_val), b_val), 
                &(*n_val, *b_val), 
                |b, &(n, batch_size)| {
                    let rt = tokio::runtime::Runtime::new().unwrap();
                    let initial_data = generate_test_data(n, dim, initial_seed);
                    
                    // This setup (building the initial index) is done ONCE per (N, B) pair,
                    // then iter_batched will clone it for each measurement.
                    let mut base_index = HnswIndex::new(hnsw_config, DistanceMetric::L2, dim).unwrap();
                    for (id, vector) in initial_data {
                        rt.block_on(base_index.add_vector(id, vector)).unwrap();
                    }

                    let mut current_id_offset = n; // Start IDs for batch after initial data
                    
                    b.iter_batched(
                        || { // Setup for each iteration: Rebuild base_index and generate a batch of B new vectors
                            let mut current_base_index = HnswIndex::new(hnsw_config, DistanceMetric::L2, dim).unwrap();
                            let current_initial_data = generate_test_data(n, dim, initial_seed); // Regenerate initial data
                            for (id, vector) in current_initial_data {
                                rt.block_on(current_base_index.add_vector(id, vector)).unwrap();
                            }

                            current_id_offset += batch_size; 
                            let batch_data_seed = initial_seed + current_id_offset as u64;
                            let batch_vectors = generate_test_data(batch_size, dim, batch_data_seed);
                            (current_base_index, batch_vectors) 
                        },
                        |(mut index_for_batch, batch_data)| { // Routine: add the batch of vectors
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
    let initial_seed = 4u64; // Yet another seed

    let n_values = [1000, 10000, 50000];
    let k_values = [1, 10, 50];
    let ef_search_values = [50, 100, 200]; // Must be >= k

    for n_val in n_values.iter() {
        for k_val in k_values.iter() {
            for ef_s_val in ef_search_values.iter() {
                if *ef_s_val < *k_val { // ef_search must be >= k
                    continue;
                }

                let bench_id_str = format!("N={}/k={}/ef_search={}", n_val, k_val, ef_s_val);
                group.throughput(Throughput::Elements(1)); // Benchmarking one search operation

                group.bench_with_input(
                    BenchmarkId::from_parameter(bench_id_str),
                    &(*n_val, *k_val, *ef_s_val),
                    |b, &(n, k_param, ef_param)| {
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        let hnsw_config = HnswConfig {
                            m: 16, m_max0: 32, ef_construction: 100, 
                            ef_search: ef_param, // Use parameterized ef_search
                            ml: 1.0 / (16.0f64.ln()), seed: Some(initial_seed)
                        };
                        
                        let data = generate_test_data(n, dim, initial_seed);
                        let mut index = HnswIndex::new(hnsw_config, DistanceMetric::L2, dim).unwrap();
                        for (id, vector) in data {
                            rt.block_on(index.add_vector(id, vector)).unwrap();
                        }

                        // Generate a query vector (can be one from the set or a new random one)
                        // For simplicity, using a new random one.
                        let mut query_rng = StdRng::seed_from_u64(initial_seed + n as u64 + 1);
                        let query_vector = generate_random_vector(dim, &mut query_rng);

                        b.iter_batched(
                            || query_vector.clone(), // Setup: clone the query vector for this iteration
                            |q_vec| { // Routine: perform the search
                                black_box(rt.block_on(index.search(black_box(q_vec), black_box(k_param))).unwrap());
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    }
                );
            }
        }
    }
    group.finish();
}

// --- Main Benchmark Registration ---

criterion_group!(benches, bench_build_index, bench_add_vector_single, bench_add_vector_batch, bench_search_latency);
criterion_main!(benches);
