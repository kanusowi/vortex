use ndarray::Array1;
use serde::{Serialize, Deserialize};

/// Type alias for vector identifiers. Chosen as String for flexibility.
pub type VectorId = String;

/// Type alias for the vector embedding representation.
/// Uses `ndarray::Array1<f32>` for efficient numerical operations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Embedding(pub Array1<f32>);

// Implement Deref to allow easy access to Array1 methods
impl std::ops::Deref for Embedding {
    type Target = Array1<f32>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Implement DerefMut
impl std::ops::DerefMut for Embedding {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// Implement From<Vec<f32>> for convenience
impl From<Vec<f32>> for Embedding {
    fn from(vec: Vec<f32>) -> Self {
        Embedding(Array1::from(vec))
    }
}

// Implement Into<Vec<f32>> for convenience
impl From<Embedding> for Vec<f32> {
    fn from(embedding: Embedding) -> Self {
        embedding.0.to_vec()
    }
}