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
    /// Dimensionality of the vectors.
    pub vector_dim: u32,
    // Add payload storage flag later if needed
    // pub store_payloads: bool,
}

impl HnswConfig {
    /// Creates a new HNSW configuration with default values derived from M.
    pub fn new(vector_dim: u32, m: usize, ef_construction: usize, ef_search: usize, ml: f64) -> Self {
        HnswConfig {
            vector_dim,
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
        if self.vector_dim == 0 {
            return Err(VortexError::Configuration("vector_dim must be greater than 0".to_string()));
        }
        Ok(())
    }
}

// Sensible default values
impl Default for HnswConfig {
    fn default() -> Self {
        HnswConfig {
            vector_dim: 0, // User must set this, or it's set during index creation. 0 is invalid.
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            ml: 1.0 / (16.0f64.ln()), // Standard heuristic based on M
            seed: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> HnswConfig {
        let mut config = HnswConfig::default();
        config.vector_dim = 128; // Set a valid dimension
        config
    }

    #[test]
    fn test_hnsw_config_validate_valid() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn test_hnsw_config_validate_invalid_m() {
        let mut config = valid_config();
        config.m = 0;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_validate_invalid_m_max0() {
        let mut config = valid_config();
        config.m_max0 = 0;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_validate_invalid_ef_construction() {
        let mut config = valid_config();
        config.ef_construction = 0;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_validate_invalid_ef_search() {
        let mut config = valid_config();
        config.ef_search = 0;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_validate_invalid_ml_zero() {
        let mut config = valid_config();
        config.ml = 0.0;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_validate_invalid_ml_negative() {
        let mut config = valid_config();
        config.ml = -0.1;
        assert!(matches!(config.validate(), Err(VortexError::Configuration(_))));
    }

    #[test]
    fn test_hnsw_config_new() {
        let dim = 128;
        let m = 10;
        let ef_c = 100;
        let ef_s = 20;
        let ml = 0.5;
        let config = HnswConfig::new(dim, m, ef_c, ef_s, ml);
        assert_eq!(config.vector_dim, dim);
        assert_eq!(config.m, m);
        assert_eq!(config.m_max0, m * 2);
        assert_eq!(config.ef_construction, ef_c);
        assert_eq!(config.ef_search, ef_s);
        assert_eq!(config.ml, ml);
        assert_eq!(config.seed, None);
    }
}
