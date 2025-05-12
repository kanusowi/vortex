use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

/// Generates a random level for a new node based on the HNSW probability formula.
/// Uses the formula: floor(-ln(uniform(0,1)) * ml)
pub(crate) fn generate_random_level(ml: f64, rng: &mut impl Rng) -> usize {
    // Generate a random float in (0.0, 1.0] - avoid exactly 0 for ln
    let uniform_random: f64 = rng.gen_range(f64::EPSILON..=1.0);
    (-uniform_random.ln() * ml).floor() as usize
}

/// Creates a seeded random number generator or a default one.
pub fn create_rng(seed: Option<u64>) -> StdRng { // Changed to pub
    match seed {
        Some(s) => StdRng::seed_from_u64(s),
        None => StdRng::from_entropy(),
    }
}
