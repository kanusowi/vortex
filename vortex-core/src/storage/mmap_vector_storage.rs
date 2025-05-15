use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; // ErrorKind removed
use std::path::{Path, PathBuf};
use std::mem::size_of;

use memmap2::{MmapMut, MmapOptions};
use ndarray::Array1;
// serde is not directly used for these headers due to repr(C, packed) and direct byte manipulation
// use serde::{Serialize, Deserialize}; 

use crate::error::VortexError;
use crate::vector::Embedding;

const CURRENT_VERSION: u16 = 1;
const DATA_FILE_MAGIC: &[u8; 6] = b"VTXVEC";
const DELETION_FILE_MAGIC: &[u8; 6] = b"VEXDEL";

/// Header for the memory-mapped vector data file.
/// Ensures C-compatible layout and no padding for direct memory operations.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct MmapFileHeader {
    magic: [u8; 6],
    version: u16,
    dimensionality: u32,
    capacity: u64,      // Max number of vectors this file can hold
    vector_count: u64,  // Current number of active (non-deleted) vectors
    reserved: [u8; 4],  // For alignment and future use (total 32 bytes)
}

impl MmapFileHeader {
    /// Creates a new header for a data file.
    #[allow(dead_code)] // Will be used by MmapVectorStorage::new
    fn new(dimensionality: u32, capacity: u64) -> Self {
        Self {
            magic: *DATA_FILE_MAGIC,
            version: CURRENT_VERSION,
            dimensionality,
            capacity,
            vector_count: 0,
            reserved: [0; 4],
        }
    }

    /// Reads a header from a byte slice.
    #[allow(dead_code)] // Will be used by MmapVectorStorage::new or load methods
    fn from_bytes(bytes: &[u8]) -> Result<Self, VortexError> {
        if bytes.len() < size_of::<Self>() {
            return Err(VortexError::StorageError("Header too short for MmapFileHeader".into()));
        }
        // Safe because MmapFileHeader is repr(C, packed) and contains only POD types.
        let header: Self = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) };
        if &header.magic != DATA_FILE_MAGIC {
            return Err(VortexError::StorageError("Invalid data file magic number".into()));
        }
        let header_version = header.version; // Copy to local variable for safe access
        if header_version > CURRENT_VERSION {
            return Err(VortexError::StorageError(format!(
                "Unsupported data file version: {} (expected <= {})",
                header_version, CURRENT_VERSION
            )));
        }
        Ok(header)
    }

    /// Returns the header as a byte slice.
    #[allow(dead_code)] // Will be used by MmapVectorStorage methods to write header
    fn as_bytes(&self) -> &[u8] {
        // Safe because MmapFileHeader is repr(C, packed).
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, size_of::<Self>())
        }
    }
}

/// Header for the memory-mapped deletion flags file.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeletionFileHeader {
    magic: [u8; 6],
    version: u16,
    capacity: u64,      // Must match vector data file capacity
    reserved: [u8; 22], // To make it 32 bytes like MmapFileHeader for consistency
}

impl DeletionFileHeader {
    /// Creates a new header for a deletion flags file.
    #[allow(dead_code)] // Will be used by MmapVectorStorage::new
    fn new(capacity: u64) -> Self {
        Self {
            magic: *DELETION_FILE_MAGIC,
            version: CURRENT_VERSION,
            capacity,
            reserved: [0; 22],
        }
    }

    /// Reads a deletion file header from a byte slice.
    #[allow(dead_code)] // Will be used by MmapVectorStorage::new or load methods
    fn from_bytes(bytes: &[u8]) -> Result<Self, VortexError> {
        if bytes.len() < size_of::<Self>() {
            return Err(VortexError::StorageError("Header too short for DeletionFileHeader".into()));
        }
        // Safe because DeletionFileHeader is repr(C, packed).
        let header: Self = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) };
        if &header.magic != DELETION_FILE_MAGIC {
            return Err(VortexError::StorageError("Invalid deletion file magic number".into()));
        }
        let header_version = header.version; // Copy to local variable
        if header_version > CURRENT_VERSION {
             return Err(VortexError::StorageError(format!(
                "Unsupported deletion file version: {} (expected <= {})",
                header_version, CURRENT_VERSION
            )));
        }
        Ok(header)
    }

    /// Returns the deletion file header as a byte slice.
    #[allow(dead_code)] // Will be used by MmapVectorStorage methods to write header
     fn as_bytes(&self) -> &[u8] {
        // Safe because DeletionFileHeader is repr(C, packed).
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, size_of::<Self>())
        }
    }
}

/// Manages memory-mapped storage for dense vectors and their deletion flags.
#[derive(Debug)]
#[allow(dead_code)] 
pub struct MmapVectorStorage {
    data_mmap: MmapMut,
    deletion_flags_mmap: MmapMut,
    header: MmapFileHeader,
    _data_file: File, 
    _deletion_flags_file: File,
    _data_file_path: PathBuf,
    _deletion_flags_file_path: PathBuf,
}

impl MmapVectorStorage {
    #[allow(dead_code)] 
    pub fn new(base_path: &Path, name: &str, dim: u32, capacity: u64) -> Result<Self, VortexError> {
        if dim == 0 || capacity == 0 {
            return Err(VortexError::Configuration("Dimension and capacity must be greater than 0".into()));
        }

        let data_file_path = base_path.join(format!("{}.vec", name));
        let deletion_file_path = base_path.join(format!("{}.del", name));

        let vector_size_bytes = dim as usize * size_of::<f32>();
        let total_data_size = size_of::<MmapFileHeader>() as u64 + capacity * vector_size_bytes as u64;
        let total_deletion_flags_size = size_of::<DeletionFileHeader>() as u64 + capacity; 

        let data_file = OpenOptions::new().read(true).write(true).create(true).open(&data_file_path)?;
        data_file.set_len(total_data_size)?;

        let mut deletion_flags_file = OpenOptions::new().read(true).write(true).create(true).open(&deletion_file_path)?;
        deletion_flags_file.set_len(total_deletion_flags_size)?;
        
        // Initialize deletion flags to 1 (deleted)
        deletion_flags_file.seek(SeekFrom::Start(size_of::<DeletionFileHeader>() as u64))?;
        let buffer = vec![1u8; capacity as usize]; // Removed mut
        deletion_flags_file.write_all(&buffer)?;
        deletion_flags_file.flush()?;


        let data_header = MmapFileHeader::new(dim, capacity);
        let mut temp_data_file_for_header = data_file.try_clone()?;
        temp_data_file_for_header.seek(SeekFrom::Start(0))?;
        temp_data_file_for_header.write_all(data_header.as_bytes())?;
        temp_data_file_for_header.flush()?;

        let deletion_header = DeletionFileHeader::new(capacity);
        let mut temp_deletion_file_for_header = deletion_flags_file.try_clone()?;
        temp_deletion_file_for_header.seek(SeekFrom::Start(0))?;
        temp_deletion_file_for_header.write_all(deletion_header.as_bytes())?;
        temp_deletion_file_for_header.flush()?;

        let data_mmap = unsafe { MmapOptions::new().map_mut(&data_file)? };
        let deletion_flags_mmap = unsafe { MmapOptions::new().map_mut(&deletion_flags_file)? };

        Ok(Self {
            data_mmap,
            deletion_flags_mmap,
            header: data_header, 
            _data_file: data_file,
            _deletion_flags_file: deletion_flags_file,
            _data_file_path: data_file_path,
            _deletion_flags_file_path: deletion_file_path,
        })
    }

    pub fn dim(&self) -> u32 {
        self.header.dimensionality
    }

    pub fn capacity(&self) -> u64 {
        self.header.capacity
    }
    
    #[allow(dead_code)] 
    pub fn len(&self) -> u64 {
        self.header.vector_count
    }

    pub fn is_empty(&self) -> bool {
        self.header.vector_count == 0
    }

    fn vector_offset(&self, internal_id: u64) -> usize {
        size_of::<MmapFileHeader>() 
            + (internal_id as usize * self.header.dimensionality as usize * size_of::<f32>())
    }

    fn deletion_flag_byte_offset(&self, internal_id: u64) -> usize {
        size_of::<DeletionFileHeader>() + internal_id as usize
    }
    
    pub fn is_deleted(&self, internal_id: u64) -> bool {
        if internal_id >= self.header.capacity {
            return true; 
        }
        let offset = self.deletion_flag_byte_offset(internal_id);
        self.deletion_flags_mmap.get(offset).map_or(true, |&byte| byte != 0)
    }

    pub fn get_vector(&self, internal_id: u64) -> Option<Embedding> {
        if internal_id >= self.header.capacity || self.is_deleted(internal_id) {
            return None;
        }

        let dim = self.header.dimensionality as usize;
        let vector_size_bytes = dim * size_of::<f32>();
        let offset = self.vector_offset(internal_id);

        if offset + vector_size_bytes > self.data_mmap.len() {
            return None; 
        }

        let byte_slice = &self.data_mmap[offset..offset + vector_size_bytes];
        
        let f32_slice: &[f32] = unsafe {
            std::slice::from_raw_parts(byte_slice.as_ptr() as *const f32, dim)
        };

        Some(Embedding(Array1::from_iter(f32_slice.iter().cloned())))
    }

    pub fn put_vector(&mut self, internal_id: u64, vector: &Embedding) -> Result<(), VortexError> {
        let header_capacity = self.header.capacity; // Copy for safe access
        if internal_id >= header_capacity {
            return Err(VortexError::StorageError(format!(
                "Internal ID {} out of bounds for capacity {}",
                internal_id, header_capacity
            )));
        }
        let header_dim = self.header.dimensionality; // Copy for safe access
        if vector.dim() != header_dim as usize {
            return Err(VortexError::Configuration(format!( 
                "Vector dimension mismatch: expected {}, got {}",
                header_dim,
                vector.dim()
            )));
        }

        let dim = header_dim as usize;
        let vector_size_bytes = dim * size_of::<f32>();
        let offset = self.vector_offset(internal_id);

        if offset + vector_size_bytes > self.data_mmap.len() {
             return Err(VortexError::StorageError("Calculated offset is out of data mmap bounds.".into()));
        }

        let data_slice_mut = &mut self.data_mmap[offset..offset + vector_size_bytes];
        let vector_f32_slice = vector.0.as_slice().ok_or_else(|| VortexError::StorageError("Failed to get vector data as slice".into()))?;
        
        let vector_byte_slice = unsafe {
            std::slice::from_raw_parts(vector_f32_slice.as_ptr() as *const u8, vector_size_bytes)
        };
        data_slice_mut.copy_from_slice(vector_byte_slice);

        let del_offset = self.deletion_flag_byte_offset(internal_id);
        if del_offset >= self.deletion_flags_mmap.len() {
            return Err(VortexError::StorageError("Calculated offset is out of deletion_flags mmap bounds.".into()));
        }
        
        let was_previously_marked_deleted = self.deletion_flags_mmap[del_offset] != 0;
        self.deletion_flags_mmap[del_offset] = 0; // Mark as not deleted

        if was_previously_marked_deleted {
             self.header.vector_count += 1; // A previously deleted or unused slot is now active
        }
        // If !was_previously_marked_deleted, it's an overwrite of an active vector, so vector_count doesn't change.
        Ok(())
    }

    /// Marks a vector as deleted. Returns true if the vector was active and is now marked as deleted.
    pub fn delete_vector(&mut self, internal_id: u64) -> Result<bool, VortexError> {
        let header_capacity = self.header.capacity; // Copy for safe access
        if internal_id >= header_capacity {
            return Err(VortexError::StorageError(format!(
                "Internal ID {} out of bounds for capacity {}",
                internal_id, header_capacity
            )));
        }

        let del_offset = self.deletion_flag_byte_offset(internal_id);
        if del_offset >= self.deletion_flags_mmap.len() {
             return Err(VortexError::StorageError("Calculated offset is out of deletion_flags mmap bounds.".into()));
        }

        let was_active = self.deletion_flags_mmap[del_offset] == 0;
        
        if was_active {
            self.deletion_flags_mmap[del_offset] = 1; // Mark as deleted
            if self.header.vector_count > 0 { 
                self.header.vector_count -= 1;
            }
            Ok(true) // Vector was active and is now deleted
        } else {
            Ok(false) // Vector was already deleted (or slot was never used)
        }
    }

    pub fn flush_data(&self) -> Result<(), VortexError> {
        self.data_mmap.flush().map_err(|e| VortexError::StorageError(format!("Failed to flush data mmap: {}", e)))
    }

    pub fn flush_deletion_flags(&self) -> Result<(), VortexError> {
        self.deletion_flags_mmap.flush().map_err(|e| VortexError::StorageError(format!("Failed to flush deletion_flags mmap: {}", e)))
    }

    /// Flushes the current in-memory header to the data mmap.
    /// This method takes `&mut self` because writing to the mmap slice requires a mutable borrow of `self.data_mmap`.
    pub fn flush_header(&mut self) -> Result<(), VortexError> {
        let header_bytes = self.header.as_bytes();
        if self.data_mmap.len() < header_bytes.len() {
            return Err(VortexError::StorageError("Data mmap is too small to write header.".into()));
        }
        self.data_mmap[..header_bytes.len()].copy_from_slice(header_bytes);
        // Use flush_range for potentially better performance if only header changed.
        self.data_mmap.flush_range(0, header_bytes.len())
            .map_err(|e| VortexError::StorageError(format!("Failed to flush header to data mmap: {}", e)))?;
        Ok(())
    }

    pub fn open(base_path: &Path, name: &str) -> Result<Self, VortexError> {
        let data_file_path = base_path.join(format!("{}.vec", name));
        let deletion_file_path = base_path.join(format!("{}.del", name));

        let data_file = OpenOptions::new()
            .read(true)
            .write(true) 
            .open(&data_file_path)
            .map_err(|e| VortexError::StorageError(format!("Failed to open data file {:?}: {}", data_file_path, e)))?;

        let mut data_header_bytes = [0u8; size_of::<MmapFileHeader>()];
        let mut temp_data_file_for_read_header = data_file.try_clone()?; 
        temp_data_file_for_read_header.seek(SeekFrom::Start(0))?;
        temp_data_file_for_read_header.read_exact(&mut data_header_bytes)
            .map_err(|e| VortexError::StorageError(format!("Failed to read data file header: {}", e)))?;
        let data_header = MmapFileHeader::from_bytes(&data_header_bytes)?;

        let deletion_flags_file = OpenOptions::new()
            .read(true)
            .write(true) 
            .open(&deletion_file_path)
            .map_err(|e| VortexError::StorageError(format!("Failed to open deletion file {:?}: {}", deletion_file_path, e)))?;

        let mut deletion_header_bytes = [0u8; size_of::<DeletionFileHeader>()];
        let mut temp_del_file_for_read_header = deletion_flags_file.try_clone()?;
        temp_del_file_for_read_header.seek(SeekFrom::Start(0))?;
        temp_del_file_for_read_header.read_exact(&mut deletion_header_bytes)
            .map_err(|e| VortexError::StorageError(format!("Failed to read deletion file header: {}", e)))?;
        let deletion_header = DeletionFileHeader::from_bytes(&deletion_header_bytes)?;

        let data_header_capacity = data_header.capacity; 
        let deletion_header_capacity = deletion_header.capacity;
        if data_header_capacity != deletion_header_capacity {
            return Err(VortexError::StorageError(
                "Data file capacity and deletion file capacity mismatch".into()
            ));
        }
        
        let data_header_dimensionality = data_header.dimensionality; 
        let expected_data_size = size_of::<MmapFileHeader>() as u64 
            + data_header_capacity * data_header_dimensionality as u64 * size_of::<f32>() as u64;
        if data_file.metadata()?.len() != expected_data_size {
            return Err(VortexError::StorageError("Data file size does not match header capacity/dimensionality.".into()));
        }

        let expected_deletion_flags_size = size_of::<DeletionFileHeader>() as u64 + deletion_header_capacity;
        if deletion_flags_file.metadata()?.len() != expected_deletion_flags_size {
            return Err(VortexError::StorageError("Deletion flags file size does not match header capacity.".into()));
        }

        let data_mmap = unsafe { MmapOptions::new().map_mut(&data_file)? };
        let deletion_flags_mmap = unsafe { MmapOptions::new().map_mut(&deletion_flags_file)? };

        Ok(Self {
            data_mmap,
            deletion_flags_mmap,
            header: data_header, 
            _data_file: data_file,
            _deletion_flags_file: deletion_flags_file,
            _data_file_path: data_file_path,
            _deletion_flags_file_path: deletion_file_path,
        })
    }
}
