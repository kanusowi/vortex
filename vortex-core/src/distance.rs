//! Defines distance metrics for comparing vectors.

use ndarray::ArrayView1; // Corrected: Removed unused Array1
use serde::{Serialize, Deserialize};
use crate::error::{VortexError, VortexResult};

/// Enum representing supported distance metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine Similarity (often represented as 1 - cosine distance).
    /// Higher values mean more similar. We'll return the similarity score directly.
    Cosine,
    /// Euclidean (L2) Distance. Lower values mean more similar.
    L2,
}

/// Calculates the distance/similarity between two vectors based on the specified metric.
///
/// # Arguments
/// * `metric` - The distance metric to use.
/// * `v1` - The first vector.
/// * `v2` - The second vector.
///
/// # Returns
/// The calculated distance/similarity score as `f32`.
/// Returns `VortexError::DimensionMismatch` if vectors have different lengths.
///
/// # Notes
/// - Cosine returns *similarity* (higher is better, range [-1, 1]).
/// - L2 returns *distance* (lower is better, range [0, inf)).
pub fn calculate_distance(
    metric: DistanceMetric,
    v1: ArrayView1<f32>,
    v2: ArrayView1<f32>,
) -> VortexResult<f32> {
    if v1.len() != v2.len() {
        return Err(VortexError::DimensionMismatch {
            expected: v1.len(),
            actual: v2.len(),
        });
    }

    match metric {
        DistanceMetric::Cosine => {
            let dot_product = v1.dot(&v2);
            let norm_v1 = v1.dot(&v1).sqrt();
            let norm_v2 = v2.dot(&v2).sqrt();

            if norm_v1 == 0.0 || norm_v2 == 0.0 {
                // Handle zero vectors - cosine is undefined, return 0 similarity
                Ok(0.0)
            } else {
                // Clamp the result to avoid floating point inaccuracies causing values slightly outside [-1, 1]
                Ok((dot_product / (norm_v1 * norm_v2)).clamp(-1.0, 1.0))
            }
        }
        DistanceMetric::L2 => {
            let diff = &v1 - &v2;
            Ok(diff.dot(&diff).sqrt())
        }
    }
}

/// Trait to encapsulate distance calculation logic.
/// This allows different index types to potentially use different distance implementations.
pub trait Distance { // Keep trait definition even if unused directly elsewhere for now
    /// Calculates the distance/similarity between two vectors.
    fn distance(&self, v1: ArrayView1<f32>, v2: ArrayView1<f32>) -> VortexResult<f32>;
}

// Implement the trait for the DistanceMetric enum
impl Distance for DistanceMetric {
    fn distance(&self, v1: ArrayView1<f32>, v2: ArrayView1<f32>) -> VortexResult<f32> {
        calculate_distance(*self, v1, v2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::arr1; // Keep arr1 for tests

    #[test]
    fn test_cosine_similarity() {
        let v1 = arr1(&[1.0, 2.0, 3.0]);
        let v2 = arr1(&[1.0, 2.0, 3.0]);
        let v3 = arr1(&[-1.0, -2.0, -3.0]);
        let v4 = arr1(&[2.0, 4.0, 6.0]);
        let v5 = arr1(&[1.0, 0.0, 0.0]);
        let v6 = arr1(&[0.0, 1.0, 0.0]);
        let zero = arr1(&[0.0, 0.0, 0.0]);

        assert!((calculate_distance(DistanceMetric::Cosine, v1.view(), v2.view()).unwrap() - 1.0).abs() < 1e-6);
        assert!((calculate_distance(DistanceMetric::Cosine, v1.view(), v3.view()).unwrap() - (-1.0)).abs() < 1e-6);
        assert!((calculate_distance(DistanceMetric::Cosine, v1.view(), v4.view()).unwrap() - 1.0).abs() < 1e-6); // Parallel vectors
        assert!((calculate_distance(DistanceMetric::Cosine, v5.view(), v6.view()).unwrap() - 0.0).abs() < 1e-6); // Orthogonal vectors
        assert!((calculate_distance(DistanceMetric::Cosine, v1.view(), zero.view()).unwrap() - 0.0).abs() < 1e-6); // Zero vector case
    }

    #[test]
    fn test_l2_distance() {
        let v1 = arr1(&[1.0, 2.0, 3.0]);
        let v2 = arr1(&[1.0, 2.0, 3.0]);
        let v3 = arr1(&[4.0, 6.0, 8.0]); // Diff: [3, 4, 5]
        let zero = arr1(&[0.0, 0.0, 0.0]);

        assert!((calculate_distance(DistanceMetric::L2, v1.view(), v2.view()).unwrap() - 0.0).abs() < 1e-6);
        // sqrt(3^2 + 4^2 + 5^2) = sqrt(9 + 16 + 25) = sqrt(50)
        assert!((calculate_distance(DistanceMetric::L2, v1.view(), v3.view()).unwrap() - 50.0f32.sqrt()).abs() < 1e-6);
        // sqrt(1^2 + 2^2 + 3^2) = sqrt(1 + 4 + 9) = sqrt(14)
        assert!((calculate_distance(DistanceMetric::L2, v1.view(), zero.view()).unwrap() - 14.0f32.sqrt()).abs() < 1e-6);
    }

     #[test]
    fn test_dimension_mismatch() {
        let v1 = arr1(&[1.0, 2.0]);
        let v2 = arr1(&[1.0, 2.0, 3.0]);

        assert!(matches!(calculate_distance(DistanceMetric::Cosine, v1.view(), v2.view()), Err(VortexError::DimensionMismatch { expected: 2, actual: 3 })));
        assert!(matches!(calculate_distance(DistanceMetric::L2, v1.view(), v2.view()), Err(VortexError::DimensionMismatch { expected: 2, actual: 3 })));
    }
}
