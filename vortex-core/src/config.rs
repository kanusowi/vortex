use serde::{Serialize, Deserialize};
use crate::error::{VortexResult, VortexError};

/// Configuration parameters for the HNSW algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HnswConfig {
    /// The maximum number of connections per node per layer.
    pub m: usize,
    /// The maximum number of connections for layer 0. Typically 2 * m.
    pub m_max0: usize,
    /// The size of the dynamic candidate list during index construction (higher means better quality, slower build).
    pub ef_construction: usize,
    /// The size of the dynamic candidate list during search (higher means better recall, slower search).
    pub ef_search: usize,
    /// Normalization factor for level generation (controls the probability distribution of levels).
    pub ml: f64,
    /// Seed for the random number generator used for level assignment. If None, uses random seed.
    pub seed: Option<u64>,
    // Add payload storage flag later if needed
    // pub store_payloads: bool,
}

impl HnswConfig {
    /// Creates a new HNSW configuration with default values derived from M.
    pub fn new(m: usize, ef_construction: usize, ef_search: usize, ml: f64) -> Self {
        HnswConfig {
            m,
            m_max0: m * 2, // Default heuristic
            ef_construction,
            ef_search,
            ml,
            seed: None, // Default to random seed
        }
    }

    /// Validates the configuration parameters.
    pub fn validate(&self) -> VortexResult<()> {
        if self.m == 0 {
            return Err(VortexError::Configuration("M must be greater than 0".to_string()));
        }
        if self.m_max0 == 0 {
            return Err(VortexError::Configuration("M_max0 must be greater than 0".to_string()));
        }
        if self.ef_construction == 0 {
            return Err(VortexError::Configuration("ef_construction must be greater than 0".to_string()));
        }
         if self.ef_search == 0 {
            return Err(VortexError::Configuration("ef_search must be greater than 0".to_string()));
        }
        if self.ml <= 0.0 {
             return Err(VortexError::Configuration("ml must be greater than 0".to_string()));
        }
        Ok(())
    }
}

// Sensible default values
impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (16.0f64.ln()), // Standard heuristic based on M
            seed: None,
        }
    }
}