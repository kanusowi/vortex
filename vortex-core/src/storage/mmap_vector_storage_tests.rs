#[cfg(test)]
mod tests {
    use super::super::mmap_vector_storage::*; // Access MmapVectorStorage and its headers
    use crate::error::VortexError;
    use crate::vector::Embedding;
    use ndarray::Array1;
    use std::fs;
    // std::path::Path removed as it's not directly used, Path is used via dir.path()
    use tempfile::tempdir;
    use std::mem::size_of;

    fn create_test_embedding(dim: u32, val: f32) -> Embedding {
        Embedding(Array1::from_elem(dim as usize, val))
    }

    #[test]
    fn test_new_mmap_vector_storage() -> Result<(), VortexError> {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let name = "test_new";
        let dim = 4;
        let capacity = 100;

        let storage = MmapVectorStorage::new(path, name, dim, capacity)?;

        assert_eq!(storage.dim(), dim);
        assert_eq!(storage.capacity(), capacity);
        assert_eq!(storage.len(), 0); // Initial length should be 0

        // Check if files were created
        let data_file_path = path.join(format!("{}.vec", name));
        let deletion_file_path = path.join(format!("{}.del", name));
        assert!(data_file_path.exists());
        assert!(deletion_file_path.exists());

        // Check file sizes
        let expected_data_size = size_of::<MmapFileHeader>() as u64 + capacity * dim as u64 * size_of::<f32>() as u64;
        assert_eq!(fs::metadata(&data_file_path)?.len(), expected_data_size);

        let expected_deletion_size = size_of::<DeletionFileHeader>() as u64 + capacity;
        assert_eq!(fs::metadata(&deletion_file_path)?.len(), expected_deletion_size);
        
        Ok(())
    }

    #[test]
    fn test_put_get_vector() -> Result<(), VortexError> {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let name = "test_put_get";
        let dim = 3;
        let capacity = 10;
        let mut storage = MmapVectorStorage::new(path, name, dim, capacity)?;

        let vec1 = create_test_embedding(dim, 1.0);
        let vec2 = create_test_embedding(dim, 2.0);

        storage.put_vector(0, &vec1)?;
        storage.put_vector(5, &vec2)?;
        
        // Test vector_count update (simplified logic in put_vector)
        // After putting two distinct vectors at new slots.
        // The current put_vector increments if was_deleted or internal_id >= self.header.vector_count
        // For id 0, was_deleted=true (initial state), vector_count becomes 1.
        // For id 5, was_deleted=true, vector_count becomes 2.
        assert_eq!(storage.len(), 2);


        let retrieved_vec1 = storage.get_vector(0).unwrap();
        assert_eq!(retrieved_vec1, vec1);

        let retrieved_vec2 = storage.get_vector(5).unwrap();
        assert_eq!(retrieved_vec2, vec2);

        assert!(storage.get_vector(1).is_none()); // Not inserted

        // Test overwrite
        let vec1_updated = create_test_embedding(dim, 1.5);
        storage.put_vector(0, &vec1_updated)?;
        let retrieved_vec1_updated = storage.get_vector(0).unwrap();
        assert_eq!(retrieved_vec1_updated, vec1_updated);
        assert_eq!(storage.len(), 2); // Length should not change on overwrite of active vector

        Ok(())
    }

    #[test]
    fn test_delete_vector() -> Result<(), VortexError> {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let name = "test_delete";
        let dim = 2;
        let capacity = 5;
        let mut storage = MmapVectorStorage::new(path, name, dim, capacity)?;

        let vec1 = create_test_embedding(dim, 1.0);
        storage.put_vector(0, &vec1)?;
        assert_eq!(storage.len(), 1);
        assert!(!storage.is_deleted(0));

        storage.delete_vector(0)?;
        assert_eq!(storage.len(), 0);
        assert!(storage.is_deleted(0));
        assert!(storage.get_vector(0).is_none());

        // Delete already deleted
        storage.delete_vector(0)?;
        assert_eq!(storage.len(), 0);
        assert!(storage.is_deleted(0));
        
        // Delete out of bounds
        assert!(storage.delete_vector(capacity).is_err());

        Ok(())
    }

    #[test]
    fn test_open_existing_storage() -> Result<(), VortexError> {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let name = "test_open";
        let dim = 4;
        let capacity = 50;

        let vec1 = create_test_embedding(dim, 3.0);
        {
            let mut storage = MmapVectorStorage::new(path, name, dim, capacity)?;
            storage.put_vector(10, &vec1)?;
            storage.flush_header()?; // Ensure header (vector_count) is written
            storage.flush_data()?;
            storage.flush_deletion_flags()?;
        } // Storage is dropped, files are closed

        let opened_storage = MmapVectorStorage::open(path, name)?;
        assert_eq!(opened_storage.dim(), dim);
        assert_eq!(opened_storage.capacity(), capacity);
        assert_eq!(opened_storage.len(), 1); // Check if vector_count was loaded correctly

        let retrieved_vec1 = opened_storage.get_vector(10).unwrap();
        assert_eq!(retrieved_vec1, vec1);
        assert!(!opened_storage.is_deleted(10));
        assert!(opened_storage.get_vector(0).is_none());

        Ok(())
    }
    
    #[test]
    fn test_put_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut storage = MmapVectorStorage::new(path, "oob", 2, 5).unwrap();
        let vec = create_test_embedding(2, 1.0);
        assert!(storage.put_vector(5, &vec).is_err()); // Exact capacity is out of bounds (0-4)
        assert!(storage.put_vector(10, &vec).is_err());
    }

    #[test]
    fn test_get_out_of_bounds() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let storage = MmapVectorStorage::new(path, "oob_get", 2, 5).unwrap();
        assert!(storage.get_vector(5).is_none());
        assert!(storage.get_vector(10).is_none());
    }

    #[test]
    fn test_dimension_mismatch_put() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let mut storage = MmapVectorStorage::new(path, "dim_mismatch", 3, 5).unwrap();
        let vec_dim2 = create_test_embedding(2, 1.0);
        assert!(storage.put_vector(0, &vec_dim2).is_err());
    }
}
