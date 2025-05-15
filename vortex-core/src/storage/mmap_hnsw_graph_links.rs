use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom}; // ErrorKind removed
use std::path::{Path, PathBuf};
use std::mem::size_of;

use memmap2::{MmapMut, MmapOptions};
// serde is not directly used for these headers due to repr(C, packed) and direct byte manipulation
// use serde::{Serialize, Deserialize}; 

use crate::error::VortexError;

const CURRENT_VERSION: u16 = 1;
const GRAPH_FILE_MAGIC: &[u8; 6] = b"VTXGRH"; // Vortex Graph

/// Header for the memory-mapped HNSW graph links file.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
// No longer pub(crate) as it's used by MmapHnswGraphLinks which is pub
// Re-evaluating visibility: MmapGraphFileHeader is an implementation detail of MmapHnswGraphLinks.
// MmapHnswGraphLinks itself is pub. The header struct can remain pub(crate) if its fields
// are not directly exposed or if it's only constructed/interpreted within this crate.
// Keeping pub(crate) for now as its methods are not pub.
pub(crate) struct MmapGraphFileHeader {
    magic: [u8; 6],
    version: u16,
    num_nodes: u64,         // Current number of nodes in the graph (capacity)
    num_layers: u16,        // Number of layers in the HNSW graph
    entry_point_node_id: u64, // ID of the entry point node
    max_connections_m0: u32, // Max connections for layer 0
    max_connections_m: u32,  // Max connections for layers > 0
    reserved: [u8; 6],      // For alignment and future use (total 32 bytes)
}

impl MmapGraphFileHeader {
    fn new(num_nodes: u64, num_layers: u16, entry_point_node_id: u64, m0: u32, m: u32) -> Self {
        Self {
            magic: *GRAPH_FILE_MAGIC,
            version: CURRENT_VERSION,
            num_nodes,
            num_layers,
            entry_point_node_id,
            max_connections_m0: m0,
            max_connections_m: m,
            reserved: [0; 6],
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, VortexError> {
        if bytes.len() < size_of::<Self>() {
            return Err(VortexError::StorageError("Header too short for MmapGraphFileHeader".into()));
        }
        let header: Self = unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) };
        if &header.magic != GRAPH_FILE_MAGIC {
            return Err(VortexError::StorageError("Invalid graph file magic number".into()));
        }
        let header_version = header.version; // Copy to local variable for safe access
        if header_version > CURRENT_VERSION {
            return Err(VortexError::StorageError(format!(
                "Unsupported graph file version: {} (expected <= {})",
                header_version, CURRENT_VERSION // Use copied variable
            )));
        }
        Ok(header)
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, size_of::<Self>())
        }
    }
}

// On-disk format for each node's links in a layer:
// - Actual number of connections for this node in this layer (e.g., u16 or u32)
// - Array of connection IDs (e.g., u64 * actual_connections)

// To manage variable-length connection lists per node, we can use an offset table per layer.
// For each layer `l`:
//   - Offset Table [num_nodes]: each entry is (u64 offset_in_data_block, u16 num_connections)
//   - Data Block: concatenated connection lists for all nodes in layer `l`.

/// Manages memory-mapped storage for HNSW graph links.
///
/// The file structure will be:
/// 1. `MmapGraphFileHeader`
/// 2. For each layer `l` from `0` up to `header.num_layers - 1`:
///    a. Layer Offset Table: `num_nodes` entries of `LayerNodeOffsetEntry`.
///    b. Layer Data Block: Concatenated lists of `u64` node IDs (connections).
#[derive(Debug)]
pub struct MmapHnswGraphLinks {
    mmap: MmapMut,
    header: MmapGraphFileHeader, // This stores the canonical view of the header after new/open
    _file: File,
    _file_path: PathBuf,
}

// TODO: Implement MmapHnswGraphLinks methods:
// pub fn new(base_path: &Path, name: &str, num_nodes: u64, num_layers: u16, entry_point: u64, m0: u32, m: u32) -> Result<Self, VortexError>
// pub fn open(base_path: &Path, name: &str) -> Result<Self, VortexError>
// pub fn get_connections(&self, node_id: u64, layer: u16) -> Option<&[u64]> // Returns a slice view into the mmap
// pub fn set_connections(&mut self, node_id: u64, layer: u16, connections: &[u64]) -> Result<(), VortexError>
// pub fn flush(&self) -> Result<(), VortexError> (Implemented)
// pub fn get_entry_point_node_id(&self) -> u64 (Implemented)
// pub fn get_num_layers(&self) -> u16 (Implemented)
// pub fn get_max_connections(&self, layer: u16) -> u32 (Implemented)

// Helper to calculate offset table size for a layer
// fn layer_offset_table_size(num_nodes: u64) -> u64 {
//     num_nodes * (size_of::<u64>() + size_of::<u16>()) as u64
// }

// Helper to calculate offset table size for a layer
fn layer_offset_table_size(num_nodes: u64) -> u64 {
    num_nodes * size_of::<LayerNodeOffsetEntry>() as u64
}

// Helper to calculate data block size for a layer (max possible size)
fn layer_data_block_max_size(num_nodes: u64, max_conns_for_layer: u32) -> u64 {
    num_nodes * max_conns_for_layer as u64 * size_of::<u64>() as u64
}

#[repr(C)] // Ensure C layout, remove 'packed' to allow natural alignment/padding for bytemuck::Pod
#[derive(Debug, Clone, Copy, Default, bytemuck::Pod, bytemuck::Zeroable)]
struct LayerNodeOffsetEntry {
    offset_in_data_block: u64, // 8 bytes
    num_connections: u16,      // 2 bytes
    _padding: [u8; 6],         // 6 bytes of padding to make total size 16 bytes (multiple of 8)
} // Total size is now 16 bytes. This struct is Pod.

impl MmapHnswGraphLinks {
    // Helper to get the start byte offset of a layer's offset table
    // Note: layer_index is 0-based.
    fn get_layer_offset_table_start_offset(&self, target_layer_index: u16) -> Result<u64, VortexError> {
        let header_num_layers = self.header.num_layers; // Copy to local variable for safe access
        if target_layer_index >= header_num_layers { // num_layers is the count of allocated layers
            return Err(VortexError::StorageError(format!("Target layer index {} out of bounds for allocated layers {}.", target_layer_index, header_num_layers)));
        }
        let mut offset = size_of::<MmapGraphFileHeader>() as u64;
        for i in 0..target_layer_index { // Sum sizes of all preceding layers
            offset += layer_offset_table_size(self.header.num_nodes);
            let max_conns = if i == 0 { self.header.max_connections_m0 } else { self.header.max_connections_m };
            offset += layer_data_block_max_size(self.header.num_nodes, max_conns);
        }
        Ok(offset)
    }

    // Helper to get the start byte offset of a layer's data block
    // Note: layer_index is 0-based.
    fn get_layer_data_block_start_offset(&self, target_layer_index: u16) -> Result<u64, VortexError> {
        let offset_table_start = self.get_layer_offset_table_start_offset(target_layer_index)?;
        Ok(offset_table_start + layer_offset_table_size(self.header.num_nodes))
    }

    pub fn new(
        base_path: &Path,
        name: &str,
        num_nodes: u64,
        initial_num_layers: u16,
        initial_entry_point: u64,
        m0: u32,
        m: u32,
    ) -> Result<Self, VortexError> {
        let file_path = base_path.join(format!("{}.graph", name));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create if it doesn't exist, otherwise open existing.
            .truncate(true) // Truncate if it exists, ensuring a fresh file.
            .open(&file_path)?;

        // `initial_num_layers` is the requested number of layers.
        // We allocate space for at least 1 layer (layer 0), even if initial_num_layers is 0.
        let num_layers_to_allocate = std::cmp::max(initial_num_layers, 1);
        
        // The header should store the number of layers for which space is actually allocated.
        let header = MmapGraphFileHeader::new(num_nodes, num_layers_to_allocate, initial_entry_point, m0, m);
        
        // Calculate precise total file size needed.
        let mut current_offset = size_of::<MmapGraphFileHeader>() as u64;
        for i in 0..num_layers_to_allocate {
            current_offset += layer_offset_table_size(num_nodes);
            let max_conns = if i == 0 { m0 } else { m };
            current_offset += layer_data_block_max_size(num_nodes, max_conns);
        }
        let total_file_size = current_offset;
        
        file.set_len(total_file_size)?;

        // Write the main header
        let mut writable_file_cursor = file.try_clone()?;
        writable_file_cursor.seek(SeekFrom::Start(0))?;
        writable_file_cursor.write_all(header.as_bytes())?;

        // Initialize all offset table entries
        current_offset = size_of::<MmapGraphFileHeader>() as u64;
        let zero_entry = LayerNodeOffsetEntry::default(); // offset=0, num_connections=0
        let zero_entry_bytes = unsafe {
            std::slice::from_raw_parts(&zero_entry as *const _ as *const u8, size_of::<LayerNodeOffsetEntry>())
        };

        for i in 0..num_layers_to_allocate {
            writable_file_cursor.seek(SeekFrom::Start(current_offset))?;
            for _ in 0..num_nodes {
                writable_file_cursor.write_all(zero_entry_bytes)?;
            }
            current_offset += layer_offset_table_size(num_nodes);
            // Data blocks are implicitly zeroed by set_len if the OS does that, or contain indeterminate data.
            // Actual connections will be written by set_connections.
            let max_conns = if i == 0 { m0 } else { m };
            current_offset += layer_data_block_max_size(num_nodes, max_conns);
        }
        writable_file_cursor.flush()?; // Ensure header and offset tables are written to disk.

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // The header field in Self should reflect the state written to disk.
        Ok(Self {
            mmap,
            header, // header now contains num_layers_to_allocate
            _file: file,
            _file_path: file_path,
        })
    }

    pub fn open(base_path: &Path, name: &str) -> Result<Self, VortexError> {
        let file_path = base_path.join(format!("{}.graph", name));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .map_err(|e| VortexError::StorageError(format!("Failed to open graph file {:?}: {}", file_path, e)))?;

        let mut header_bytes = [0u8; size_of::<MmapGraphFileHeader>()];
        let mut temp_file_for_read_header = file.try_clone()?;
        temp_file_for_read_header.seek(SeekFrom::Start(0))?;
        temp_file_for_read_header.read_exact(&mut header_bytes)?;
        let header = MmapGraphFileHeader::from_bytes(&header_bytes)?;
        
        // TODO: Validate file size against header information.
        // let expected_size = ... calculate based on header.num_nodes, header.num_layers etc. ...
        // if file.metadata()?.len() < expected_size { // Should be at least, or exact if not dynamically sized
        //     return Err(VortexError::StorageError("Graph file size mismatch".into()));
        // }
        // For now, we'll skip strict file size validation on open, assuming `new` sized it correctly.
        // A robust implementation would calculate expected size from header and check.

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(Self {
            mmap,
            header,
            _file: file,
            _file_path: file_path,
        })
    }

    pub fn flush(&self) -> Result<(), VortexError> {
        self.mmap.flush().map_err(|e| VortexError::StorageError(format!("Failed to flush graph mmap: {}", e)))
    }
    
    /// Updates the entry point node ID in the header and flushes the header to disk.
    pub fn set_entry_point_node_id(&mut self, entry_point_id: u64) -> Result<(), VortexError> {
        self.header.entry_point_node_id = entry_point_id;
        // Write the updated header back to the mmap
        let header_bytes = self.header.as_bytes();
        if self.mmap.len() < header_bytes.len() {
            return Err(VortexError::StorageError("Mmap too small to write header.".into()));
        }
        self.mmap[..header_bytes.len()].copy_from_slice(header_bytes);
        // It's good practice to flush after a header update if immediate persistence is desired.
        // However, the main flush() method can be called separately.
        // For now, let HnswIndex manage when to call the main flush().
        // Consider if this specific method should flush just the header part of the mmap.
        // self.mmap.flush_async_range(0, header_bytes.len()) // or flush_range
        Ok(())
    }
    
    /// Updates the number of layers in the header and flushes the header to disk.
    /// Note: This only updates the header field. It does NOT reallocate or resize the file.
    /// The file must have been created with enough capacity for `new_num_layers`.
    pub fn set_num_layers(&mut self, new_num_layers: u16) -> Result<(), VortexError> {
        // Potentially add a check: new_num_layers should not exceed capacity implied by file size.
        // For now, assume HnswIndex manages this correctly.
        if new_num_layers == 0 && self.header.num_nodes > 0 { // An index with nodes must have at least 1 layer.
             return Err(VortexError::InvalidArgument("Number of layers cannot be set to 0 for a non-empty graph.".to_string()));
        }
        self.header.num_layers = new_num_layers;
        let header_bytes = self.header.as_bytes();
        if self.mmap.len() < header_bytes.len() {
            return Err(VortexError::StorageError("Mmap too small to write header.".into()));
        }
        self.mmap[..header_bytes.len()].copy_from_slice(header_bytes);
        Ok(())
    }

    pub fn get_entry_point_node_id(&self) -> u64 {
        self.header.entry_point_node_id
    }

    pub fn get_num_layers(&self) -> u16 {
        self.header.num_layers
    }

    pub fn get_max_connections(&self, layer: u16) -> Result<u32, VortexError> {
        let num_layers = self.header.num_layers; // Copy to local variable
        if layer >= num_layers {
            return Err(VortexError::StorageError(format!("Layer {} out of bounds for num_layers {}", layer, num_layers)));
        }
        Ok(if layer == 0 { self.header.max_connections_m0 } else { self.header.max_connections_m })
    }

    pub fn get_connections(&self, node_id: u64, layer_index: u16) -> Option<&[u64]> {
        // Bounds checks
        if layer_index >= self.header.num_layers {
            // Using tracing::warn or similar for internal logic errors might be good.
            // For now, just return None as per Option semantics.
            return None;
        }
        if node_id >= self.header.num_nodes {
            return None;
        }

        // Calculate offset to the start of the specified layer's offset table.
        let layer_offset_table_start = match self.get_layer_offset_table_start_offset(layer_index) {
            Ok(offset) => offset,
            Err(_) => return None, // Should not happen if layer_index < self.header.num_layers
        };

        // Calculate the offset of the LayerNodeOffsetEntry for the given node_id.
        let entry_offset_in_file = layer_offset_table_start + (node_id * size_of::<LayerNodeOffsetEntry>() as u64);
        
        // Ensure we can read the LayerNodeOffsetEntry itself.
        let entry_end_offset = entry_offset_in_file + size_of::<LayerNodeOffsetEntry>() as u64;
        if entry_end_offset > self.mmap.len() as u64 {
            // Offset table entry itself is out of bounds of the mmap file.
            return None; 
        }

        // Read the LayerNodeOffsetEntry.
        // This is unsafe because we are casting a raw pointer from the mmap.
        // We've checked bounds, but alignment and struct validity are concerns.
        // repr(C, packed) helps, bytemuck would be safer if LayerNodeOffsetEntry derived Pod.
        let entry_bytes = &self.mmap[entry_offset_in_file as usize .. entry_end_offset as usize];
        let entry: LayerNodeOffsetEntry = match bytemuck::try_from_bytes(entry_bytes) {
            Ok(&e) => e,
            Err(_) => {
                // This indicates a serious issue, either data corruption or programming error.
                // Consider logging this error.
                return None;
            }
        };
        
        if entry.num_connections == 0 {
            return Some(&[]); // No connections, return an empty slice.
        }

        // Calculate offset to the start of this layer's data block.
        let layer_data_block_start = match self.get_layer_data_block_start_offset(layer_index) {
            Ok(offset) => offset,
            Err(_) => return None, // Should not happen
        };
        
        // Calculate the start and end byte offsets for the connections slice within the mmap.
        let connections_start_byte = layer_data_block_start + entry.offset_in_data_block;
        let connections_byte_len = entry.num_connections as u64 * size_of::<u64>() as u64;
        let connections_end_byte = connections_start_byte + connections_byte_len;

        // Bounds check for the connections slice itself.
        if connections_end_byte > self.mmap.len() as u64 {
            // Connections data is out of bounds of the mmap file.
            // This indicates corruption or an invalid LayerNodeOffsetEntry.
            return None;
        }
        
        let connections_byte_slice = &self.mmap[connections_start_byte as usize .. connections_end_byte as usize];
        
        // Safely cast the byte slice to &[u64] using bytemuck.
        match bytemuck::try_cast_slice(connections_byte_slice) {
            Ok(slice) => Some(slice),
            Err(_) => {
                // This means the slice isn't properly aligned or sized for &[u64].
                // Indicates corruption or programming error.
                None
            }
        }
    }

    pub fn set_connections(&mut self, node_id: u64, layer_index: u16, connections: &[u64]) -> Result<(), VortexError> {
        // Bounds checks
        let header_num_layers = self.header.num_layers; // Copy for safe access
        let header_num_nodes = self.header.num_nodes; // Copy for safe access

        if layer_index >= header_num_layers {
            return Err(VortexError::InvalidArgument(format!("Layer index {} out of bounds for allocated layers {}.", layer_index, header_num_layers)));
        }
        if node_id >= header_num_nodes {
            return Err(VortexError::InvalidArgument(format!("Node ID {} out of bounds for num_nodes {}.", node_id, header_num_nodes)));
        }
        
        let max_conns_for_layer = self.get_max_connections(layer_index)?; // This also checks layer_index bounds again
        if connections.len() > max_conns_for_layer as usize {
            return Err(VortexError::InvalidArgument(format!(
                "Number of connections {} exceeds maximum {} for layer {}.",
                connections.len(), max_conns_for_layer, layer_index
            )));
        }

        // Calculate offset to the start of the specified layer's offset table.
        let layer_offset_table_start = self.get_layer_offset_table_start_offset(layer_index)?;
        
        // Calculate the offset of the LayerNodeOffsetEntry for the given node_id.
        let entry_offset_in_file = layer_offset_table_start + (node_id * size_of::<LayerNodeOffsetEntry>() as u64);
        let entry_end_offset = entry_offset_in_file + size_of::<LayerNodeOffsetEntry>() as u64;

        if entry_end_offset > self.mmap.len() as u64 {
             return Err(VortexError::StorageError("Offset table entry out of mmap bounds.".into()));
        }

        // Calculate offset to the start of this layer's data block.
        let layer_data_block_start = self.get_layer_data_block_start_offset(layer_index)?;

        // Determine the target `offset_in_data_block` for this `node_id`'s connections.
        // This assumes connections for each node are stored contiguously in pre-allocated slots.
        let node_slot_offset_in_data_block = node_id * max_conns_for_layer as u64 * size_of::<u64>() as u64;
        
        // Calculate the absolute start byte for this node's connection slot in the mmap.
        let connections_slot_start_byte = layer_data_block_start + node_slot_offset_in_data_block;
        let connections_slot_max_byte_len = max_conns_for_layer as u64 * size_of::<u64>() as u64;
        let connections_slot_end_byte = connections_slot_start_byte + connections_slot_max_byte_len;

        if connections_slot_end_byte > self.mmap.len() as u64 {
            return Err(VortexError::StorageError("Connections data slot out of mmap bounds.".into()));
        }

        // Write the connections data.
        // Get a mutable slice of u64 from the mmap corresponding to the slot.
        let target_slot_u64_len = max_conns_for_layer as usize;
        let target_mmap_u64_slice: &mut [u64] = {
            let byte_slice = &mut self.mmap[connections_slot_start_byte as usize .. connections_slot_end_byte as usize];
            // This cast is safe because we've allocated space for `max_conns_for_layer` u64s,
            // and the slot start is aligned with u64 if data block start is.
            // We should ensure data block start is u64 aligned.
            // For now, assuming alignment is handled by mmap and overall structure.
            match bytemuck::try_cast_slice_mut(byte_slice) {
                Ok(slice) => slice,
                Err(_) => return Err(VortexError::StorageError("Failed to cast mmap slice to &mut [u64] for connections.".into())),
            }
        };
        
        // Copy the actual connections.
        target_mmap_u64_slice[..connections.len()].copy_from_slice(connections);
        
        // Zero out the rest of the pre-allocated slot if connections list is shorter than max.
        if connections.len() < target_slot_u64_len {
            for i in connections.len()..target_slot_u64_len {
                target_mmap_u64_slice[i] = 0;
            }
        }

        // Update the LayerNodeOffsetEntry.
        let updated_entry = LayerNodeOffsetEntry {
            offset_in_data_block: node_slot_offset_in_data_block,
            num_connections: connections.len() as u16,
            _padding: [0; 6], // Initialize padding
        };
        let entry_bytes_to_write = bytemuck::bytes_of(&updated_entry);
        
        self.mmap[entry_offset_in_file as usize .. entry_end_offset as usize].copy_from_slice(entry_bytes_to_write);

        // Note: Updating header.num_layers (if it means "populated layers") or header.entry_point_node_id
        // is typically managed by HnswIndex logic, not directly here.
        // This MmapHnswGraphLinks struct manages the raw link storage based on initial capacity.
        // If `self.header.num_layers` was meant to be dynamic (actual populated layers),
        // then it would need to be updated here and flushed. However, current design uses it
        // as "allocated layers" from `new()`.

        Ok(())
    }
}
