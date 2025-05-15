use crate::wal::mmap_utils::MmapViewSync;
use byteorder::{ByteOrder, LittleEndian};
use rand; 
use rustix; 
use tracing::{debug, warn, trace, error}; // Use tracing macros
use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;
use std::path::{Path, PathBuf};
// use std::thread; // No longer needed after flush_async change
#[cfg(target_os = "windows")]
use std::time::Duration;

#[cfg(not(unix))]
use fs4::fs_std::FileExt; // For Windows file allocation if needed, or remove if not directly used

/// The magic bytes and version tag of the VortexSegment header.
const VORTEX_SEGMENT_MAGIC: &[u8; 3] = b"VXW"; // VortexWAL
const VORTEX_SEGMENT_VERSION: u8 = 0;

/// The length of both the segment and entry header.
const HEADER_LEN: usize = 8; // Magic (3) + Version (1) + CRC Seed (4) for segment; Length (8) for entry

/// The length of a CRC value.
const CRC_LEN: usize = 4; // CRC32-C

pub struct VortexEntry {
    view: MmapViewSync,
}

impl Deref for VortexEntry {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { self.view.as_slice() }
    }
}

impl fmt::Debug for VortexEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VortexEntry {{ len: {} }}", self.view.len())
    }
}

pub struct VortexSegment {
    mmap: MmapViewSync,
    path: PathBuf,
    index: Vec<(usize, usize)>, // offset, length
    crc: u32,                   // Current chained CRC32-C value
    flush_offset: usize,
}

impl VortexSegment {
    pub fn create<P>(path: P, capacity: usize) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Segment path has no filename"))?;

        let tmp_file_path = match path.as_ref().parent() {
            Some(parent) => parent.join(format!("tmp-{}", file_name)),
            None => PathBuf::from(format!("tmp-{}", file_name)),
        };

        let capacity = capacity & !7; // Align to 8 bytes
        if capacity < HEADER_LEN {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Invalid segment capacity: {}", capacity),
            ));
        }
        let seed = rand::random::<u32>(); // Random CRC seed for this segment

        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false) // We'll set len via ftruncate/allocate
                .open(&tmp_file_path)?;

            #[cfg(unix)]
            rustix::fs::ftruncate(&file, capacity as u64).map_err(|e| Error::new(ErrorKind::Other, format!("ftruncate failed: {}", e)))?;
            #[cfg(not(unix))] // e.g. Windows
            file.allocate(capacity as u64)?;


            let mut mmap = MmapViewSync::from_file(&file, 0, capacity)?;
            {
                let segment_slice = unsafe { mmap.as_mut_slice() };
                copy_memory(VORTEX_SEGMENT_MAGIC, segment_slice);
                segment_slice[3] = VORTEX_SEGMENT_VERSION;
                LittleEndian::write_u32(&mut segment_slice[4..HEADER_LEN], seed);
            }
            mmap.flush()?; // Ensure header is written

            #[cfg(target_os = "windows")]
            file.sync_all()?;
        }

        fs::rename(&tmp_file_path, path.as_ref())?;

        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let segment = Self {
            mmap,
            path: path.as_ref().to_path_buf(),
            index: Vec::new(),
            crc: seed,
            flush_offset: HEADER_LEN, // Header is flushed
        };
        debug!("{:?}: created", segment);
        Ok(segment)
    }

    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
        let file_capacity = file.metadata()?.len();
        if file_capacity > usize::MAX as u64 || file_capacity < HEADER_LEN as u64 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Invalid segment capacity from file metadata: {}", file_capacity),
            ));
        }

        let capacity = file_capacity as usize & !7;
        let mmap = MmapViewSync::from_file(&file, 0, capacity)?;

        let mut index = Vec::new();
        let mut current_crc;
        let final_offset; // To store the offset value after the loop

        {
            let segment_slice = unsafe { mmap.as_slice() };

            if &segment_slice[0..3] != VORTEX_SEGMENT_MAGIC {
                return Err(Error::new(ErrorKind::InvalidData, "Illegal segment magic bytes"));
            }
            if segment_slice[3] != VORTEX_SEGMENT_VERSION {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unsupported segment version: {}", segment_slice[3]),
                ));
            }

            current_crc = LittleEndian::read_u32(&segment_slice[4..HEADER_LEN]);
            let mut offset_val = HEADER_LEN; // Renamed to avoid conflict, and make it mutable for the loop

            while offset_val + HEADER_LEN + CRC_LEN <= capacity { 
                let data_len = LittleEndian::read_u64(&segment_slice[offset_val..offset_val + HEADER_LEN]) as usize;
                let padding_len = padding(data_len);
                let padded_data_len = data_len + padding_len;
                
                if offset_val + HEADER_LEN + padded_data_len + CRC_LEN > capacity {
                    break; 
                }

                let data_to_crc = &segment_slice[offset_val .. offset_val + HEADER_LEN + padded_data_len];
                let entry_calculated_crc = crc32c::crc32c_append(!current_crc.reverse_bits(), data_to_crc);
                
                let stored_crc_offset = offset_val + HEADER_LEN + padded_data_len;
                let entry_stored_crc = LittleEndian::read_u32(&segment_slice[stored_crc_offset .. stored_crc_offset + CRC_LEN]);

                if entry_calculated_crc != entry_stored_crc {
                    if entry_stored_crc != 0 { 
                        warn!(
                            "CRC mismatch in segment {:?} at entry offset {}: calculated {}, stored {}. Truncating here.",
                            path.as_ref(), offset_val, entry_calculated_crc, entry_stored_crc
                        );
                    }
                    break; // Break before pushing to index if CRC fails
                }

                // If CRC matches, then add to index and update CRC
                current_crc = entry_calculated_crc; 
                index.push((offset_val + HEADER_LEN, data_len)); 
                offset_val += HEADER_LEN + padded_data_len + CRC_LEN; 
            }
            final_offset = offset_val; // Assign the final value of offset_val
        }

        let segment = Self {
            mmap,
            path: path.as_ref().to_path_buf(),
            index,
            crc: current_crc,
            flush_offset: final_offset, // Use the final_offset value here
        };
        debug!("{:?}: opened", segment);
        Ok(segment)
    }
    
    pub fn entry(&self, entry_index: usize) -> Option<VortexEntry> {
        self.index.get(entry_index).map(|&(data_offset, data_len)| {
            let mut view = unsafe { self.mmap.clone() };
            view.restrict(data_offset, data_len)
                .expect("Internal error: segment index contains invalid offset/length");
            VortexEntry { view }
        })
    }

    pub fn append_record_bytes(&mut self, record_bytes: &[u8]) -> Option<usize> {
        if !self.sufficient_capacity(record_bytes.len()) {
            return None;
        }
        trace!("{:?}: appending {} byte entry", self, record_bytes.len());

        let data_len = record_bytes.len();
        let padding_len = padding(data_len);
        let padded_data_len = data_len + padding_len;
        
        let current_write_offset = self.current_size(); 

        LittleEndian::write_u64(&mut self.as_mut_slice()[current_write_offset .. current_write_offset + HEADER_LEN], data_len as u64);
        
        let data_start_offset = current_write_offset + HEADER_LEN;
        copy_memory(record_bytes, &mut self.as_mut_slice()[data_start_offset .. data_start_offset + data_len]);

        if padding_len > 0 {
            let zeros: [u8; 8] = [0; 8]; 
            copy_memory(&zeros[..padding_len], &mut self.as_mut_slice()[data_start_offset + data_len .. data_start_offset + padded_data_len]);
        }

        let data_to_crc = &self.as_slice()[current_write_offset .. current_write_offset + HEADER_LEN + padded_data_len];
        let new_crc = crc32c::crc32c_append(!self.crc.reverse_bits(), data_to_crc);
        
        let crc_offset = current_write_offset + HEADER_LEN + padded_data_len;
        LittleEndian::write_u32(&mut self.as_mut_slice()[crc_offset .. crc_offset + CRC_LEN], new_crc);

        self.crc = new_crc; 
        self.index.push((data_start_offset, data_len)); 
        Some(self.index.len() - 1) 
    }

    pub fn truncate_from_ordinal(&mut self, from_entry_ordinal: usize) {
        if from_entry_ordinal >= self.index.len() {
            return;
        }
        trace!("{:?}: truncating from ordinal entry {}", self, from_entry_ordinal);

        let _deleted_count = self.index.drain(from_entry_ordinal..).count();
        
        if self.index.is_empty() {
            let segment_slice = unsafe { self.mmap.as_slice() };
            self.crc = LittleEndian::read_u32(&segment_slice[4..HEADER_LEN]);
        } else {
            let (last_entry_data_offset, last_entry_data_len) = self.index[self.index.len() - 1];
            let last_entry_header_offset = last_entry_data_offset - HEADER_LEN;
            let last_entry_padding_len = padding(last_entry_data_len);
            let last_entry_padded_data_len = last_entry_data_len + last_entry_padding_len;
            let last_entry_crc_offset = last_entry_header_offset + HEADER_LEN + last_entry_padded_data_len;
            
            let segment_slice = unsafe { self.mmap.as_slice() };
            self.crc = LittleEndian::read_u32(&segment_slice[last_entry_crc_offset .. last_entry_crc_offset + CRC_LEN]);
        }

        let new_size = self.current_size();
        let capacity = self.capacity();
        if new_size < capacity {
            let mut_slice = unsafe { self.mmap.as_mut_slice() };
            for byte in &mut mut_slice[new_size..capacity] {
                *byte = 0;
            }
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        trace!("{:?}: flushing", self);
        let start = self.flush_offset;
        let end = self.current_size(); 

        match start.cmp(&end) {
            Ordering::Equal => {
                trace!("{:?}: nothing to flush", self);
                Ok(())
            }
            Ordering::Less => {
                trace!("{:?}: flushing byte range [{}, {})", self, start, end);
                let mut view = unsafe { self.mmap.clone() };
                view.restrict(start, end - start)?; 
                view.flush()?;
                self.flush_offset = end;
                Ok(())
            }
            Ordering::Greater => { 
                trace!("{:?}: flushing after truncation (full segment flush)", self);
                let mut view = unsafe { self.mmap.clone() };
                view.restrict(0, end)?; 
                view.flush()?;
                self.flush_offset = end;
                Ok(())
            }
        }
    }
    
    pub fn ensure_capacity(&mut self, required_entry_data_len: usize) -> Result<()> {
        let required_total_entry_len = total_space_for_entry(required_entry_data_len);
        let needed_capacity = self.current_size() + required_total_entry_len;
        
        if needed_capacity > self.capacity() {
            let new_capacity = (needed_capacity.next_power_of_two()).max(self.capacity() * 2); 
            let new_capacity_aligned = new_capacity & !7; 
            debug!("{:?}: resizing from {} to {} bytes", self, self.capacity(), new_capacity_aligned);
            
            self.flush()?; 
            
            let file = OpenOptions::new().read(true).write(true).open(&self.path)?;

            #[cfg(unix)]
            rustix::fs::ftruncate(&file, new_capacity_aligned as u64).map_err(|e| Error::new(ErrorKind::Other, format!("ftruncate failed: {}", e)))?;
            #[cfg(not(unix))]
            file.allocate(new_capacity_aligned as u64)?;

            let new_mmap = MmapViewSync::from_file(&file, 0, new_capacity_aligned)?;
            self.mmap = new_mmap; 
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    pub fn current_size(&self) -> usize {
        self.index.last().map_or(HEADER_LEN, |&(data_offset, data_len)| {
            data_offset + data_len + padding(data_len) + CRC_LEN
        })
    }

    pub fn sufficient_capacity(&self, entry_data_len: usize) -> bool {
        let required_total_space = total_space_for_entry(entry_data_len);
        self.capacity() >= self.current_size() + required_total_space
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn rename<P>(&mut self, new_path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        debug!("{:?}: renaming file to {:?}", self, new_path.as_ref());
        fs::rename(&self.path, new_path.as_ref()).map_err(|e| {
            error!("Failed to rename segment {:?} to {:?}: {}", self.path, new_path.as_ref(), e);
            e
        })?;
        self.path = new_path.as_ref().to_path_buf();
        Ok(())
    }

    pub fn delete(self) -> Result<()> {
        debug!("{:?}: deleting file", self);
        let path_to_delete = self.path.clone();
        drop(self.mmap); 

        fs::remove_file(&path_to_delete).map_err(|e| {
            error!("Failed to delete segment {:?}: {}", path_to_delete, e);
            e
        })
    }
    
    fn as_slice(&self) -> &[u8] {
        unsafe { self.mmap.as_slice() }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { self.mmap.as_mut_slice() }
    }

    /// Calculates the total space an entry with the given data length would occupy on disk.
    /// This includes the entry header, data, padding, and CRC.
    pub fn on_disk_size(data_len: usize) -> usize {
        total_space_for_entry(data_len)
    }

    /// Initiates an asynchronous flush of outstanding modifications to disk.
    /// This method offloads the flush operation to a blocking thread pool
    /// and returns once the operation is successfully submitted or fails.
    /// The actual data persistence is handled by the OS in the background.
    pub async fn flush_async(&mut self) -> Result<()> {
        trace!("{:?}: initiating async flush", self);
        let start_offset_to_flush = self.flush_offset;
        let end_offset_to_flush = self.current_size();

        let flush_range_start;
        let flush_range_len;

        match start_offset_to_flush.cmp(&end_offset_to_flush) {
            Ordering::Equal => {
                trace!("{:?}: nothing to flush (async)", self);
                return Ok(());
            }
            Ordering::Less => {
                trace!("{:?}: preparing async flush for byte range [{}, {})", self, start_offset_to_flush, end_offset_to_flush);
                flush_range_start = start_offset_to_flush;
                flush_range_len = end_offset_to_flush - start_offset_to_flush;
            }
            Ordering::Greater => { // This case implies truncation happened.
                trace!("{:?}: preparing async flush after truncation (full segment up to current size {})", self, end_offset_to_flush);
                flush_range_start = 0; // Flush from the beginning of the mmap view
                flush_range_len = end_offset_to_flush; // Up to the new (smaller) end
            }
        }

        // Clone the MmapViewSync for use in the blocking task.
        // The MmapViewSync's internal offset and len define its view of the underlying mmap.
        // We need to restrict this cloned view to the specific part we want to flush.
        let mmap_view_for_task = unsafe { self.mmap.clone() };

        let task_result = tokio::task::spawn_blocking(move || {
            let mut view_to_flush = mmap_view_for_task; // This is the cloned MmapViewSync
            // `restrict` adjusts the view_to_flush.offset and view_to_flush.len
            // relative to its *current* view.
            // If self.mmap views the whole segment (offset 0, len capacity),
            // then restrict(flush_range_start, flush_range_len) correctly sets the sub-view.
            if let Err(e) = view_to_flush.restrict(flush_range_start, flush_range_len) {
                return Err(e);
            }
            view_to_flush.flush_async_os()
        }).await;

        match task_result {
            Ok(Ok(())) => { // spawn_blocking succeeded, and flush_async_os succeeded
                self.flush_offset = end_offset_to_flush;
                Ok(())
            }
            Ok(Err(io_err)) => Err(io_err), // spawn_blocking succeeded, but flush_async_os failed
            Err(join_err) => { // spawn_blocking (task itself) failed, e.g., panicked
                Err(Error::new(ErrorKind::Other, format!("Async flush task failed: {}", join_err)))
            }
        }
    }
}

impl fmt::Debug for VortexSegment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VortexSegment {{ path: {:?}, entries: {}, size: {}/{}, crc_seed_or_last: 0x{:x} }}",
            &self.path,
            self.len(),
            self.current_size(),
            self.capacity(),
            self.crc
        )
    }
}

fn copy_memory(src: &[u8], dst: &mut [u8]) {
    let len_src = src.len();
    assert!(dst.len() >= len_src, "Destination slice is too short.");
    dst[..len_src].copy_from_slice(src);
}

pub(crate) fn padding(data_len: usize) -> usize {
    (4usize.wrapping_sub(data_len)) & 7
}

fn total_space_for_entry(data_len: usize) -> usize {
    HEADER_LEN + data_len + padding(data_len) + CRC_LEN
}

pub fn entry_overhead(data_len: usize) -> usize {
    HEADER_LEN + padding(data_len) + CRC_LEN 
}

pub fn segment_header_overhead() -> usize {
    HEADER_LEN 
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir; 

    #[test]
    fn test_padding_logic() {
        assert_eq!(padding(0), 4); 
        assert_eq!(padding(1), 3); 
        assert_eq!(padding(2), 2); 
        assert_eq!(padding(3), 1); 
        assert_eq!(padding(4), 0); 
        assert_eq!(padding(5), 7); 
        assert_eq!(padding(8), 4); 
    }

    #[test]
    fn test_segment_create_open_empty() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("segment_empty.vxw"); 

        let created_segment = VortexSegment::create(&path, 1024)?;
        assert_eq!(created_segment.len(), 0);
        assert!(created_segment.is_empty());
        assert_eq!(created_segment.capacity(), 1024);
        assert_eq!(created_segment.current_size(), segment_header_overhead());
        
        let opened_segment = VortexSegment::open(&path)?;
        assert_eq!(opened_segment.len(), 0);
        assert!(opened_segment.is_empty());
        assert_eq!(opened_segment.capacity(), 1024);
        assert_eq!(opened_segment.current_size(), segment_header_overhead());
        assert_eq!(created_segment.crc, opened_segment.crc); 
        Ok(())
    }

    #[test]
    fn test_segment_append_and_read_entries() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("segment_data.vxw");
        let mut segment = VortexSegment::create(&path, 1024)?;

        let entry1_data = b"hello";
        let entry2_data = b"vortex world";

        let idx1 = segment.append_record_bytes(entry1_data).expect("Append failed");
        assert_eq!(idx1, 0);
        assert_eq!(segment.len(), 1);

        let idx2 = segment.append_record_bytes(entry2_data).expect("Append failed");
        assert_eq!(idx2, 1);
        assert_eq!(segment.len(), 2);

        let read_entry1 = segment.entry(0).unwrap();
        assert_eq!(&*read_entry1, entry1_data);

        let read_entry2 = segment.entry(1).unwrap();
        assert_eq!(&*read_entry2, entry2_data);
        
        let _expected_size = segment_header_overhead() + 
                            entry_overhead(entry1_data.len()) + entry1_data.len() +
                            entry_overhead(entry2_data.len()) + entry2_data.len();
        // This assertion was problematic due to how current_size was calculated vs entry_overhead.
        // current_size is the end offset of the last record.
        // entry_overhead is HEADER_LEN + padding + CRC_LEN.
        // A record on disk is: EntryHeader(8) + Data(N) + Padding(P) + CRC(4)
        // The index stores (offset_of_data, len_of_data)
        // current_size = last_entry_data_offset + last_entry_data_len + padding(last_entry_data_len) + CRC_LEN
        // This should be correct.
        // The issue might be if entry_overhead is used to sum up sizes.
        // Let's re-verify current_size calculation.
        // current_size = self.index.last().map_or(HEADER_LEN, |&(data_offset, data_len)| {
        //     data_offset + data_len + padding(data_len) + CRC_LEN
        // })
        // This is the offset of the *start* of data + data_len + padding + CRC.
        // This seems correct for the end of the last record.

        // Let's trace:
        // Seg Header: 8 bytes
        // Entry 1 (hello, 5 bytes): Header (8) + Data (5) + Padding (padding(5)=7) + CRC (4) = 24 bytes. Data offset = 8+8=16.
        //   current_size after entry 1 = 16 (data_offset) + 5 (data_len) + 7 (padding) + 4 (CRC) = 32.
        // Entry 2 (vortex world, 12 bytes): Header (8) + Data (12) + Padding (padding(12)=0) + CRC (4) = 24 bytes. Data offset = 32+8=40.
        //   current_size after entry 2 = 40 (data_offset) + 12 (data_len) + 0 (padding) + 4 (CRC) = 56.
        // expected_size = 8 (seg_header) + (8+5+7+4) + (8+12+0+4) = 8 + 24 + 24 = 56. Matches.
        assert_eq!(segment.current_size(), 56);


        Ok(())
    }
    
    #[test]
    fn test_segment_reopen_with_data() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("segment_reopen.vxw");
        let entry1_data = b"test_data_1";
        let entry2_data = b"another_entry_for_testing";
        let original_crc;

        {
            let mut segment = VortexSegment::create(&path, 1024)?;
            segment.append_record_bytes(entry1_data).unwrap();
            segment.append_record_bytes(entry2_data).unwrap();
            original_crc = segment.crc;
            segment.flush()?; 
        } 

        let reopened_segment = VortexSegment::open(&path)?;
        assert_eq!(reopened_segment.len(), 2);
        assert_eq!(&*reopened_segment.entry(0).unwrap(), entry1_data);
        assert_eq!(&*reopened_segment.entry(1).unwrap(), entry2_data);
        assert_eq!(reopened_segment.crc, original_crc, "Chained CRC mismatch after reopen");

        Ok(())
    }

    #[test]
    fn test_segment_truncate() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("segment_truncate.vxw");
        let mut segment = VortexSegment::create(&path, 1024)?;

        let entries = vec![
            b"entry0".to_vec(), b"entry1".to_vec(), b"entry2".to_vec(), 
            b"entry3".to_vec(), b"entry4".to_vec()
        ];
        for entry_data in &entries {
            segment.append_record_bytes(entry_data).unwrap();
        }
        assert_eq!(segment.len(), 5);
        let crc_after_2 = segment.entry(2).map(|_| segment._read_entry_crc_at_ordinal(2)).unwrap();


        segment.truncate_from_ordinal(3); 
        assert_eq!(segment.len(), 3);
        assert!(segment.entry(3).is_none());
        assert!(segment.entry(4).is_none());
        assert_eq!(&*segment.entry(2).unwrap(), entries[2].as_slice());
        assert_eq!(segment.crc, crc_after_2, "CRC should be that of the last valid entry");

        segment.truncate_from_ordinal(0); 
        assert_eq!(segment.len(), 0);
        assert!(segment.is_empty());
        let seed_crc = { 
            let segment_slice = unsafe { segment.mmap.as_slice() };
            LittleEndian::read_u32(&segment_slice[4..HEADER_LEN])
        };
        assert_eq!(segment.crc, seed_crc, "CRC should reset to seed if all entries truncated");
        
        Ok(())
    }

    impl VortexSegment {
        fn _read_entry_crc_at_ordinal(&self, entry_ordinal: usize) -> u32 {
            let (data_offset, data_len) = self.index[entry_ordinal];
            let header_offset = data_offset - HEADER_LEN; 
            let padding_len = padding(data_len);
            let padded_data_len = data_len + padding_len;
            let crc_offset = header_offset + HEADER_LEN + padded_data_len;
            
            let segment_slice = unsafe { self.mmap.as_slice() };
            LittleEndian::read_u32(&segment_slice[crc_offset .. crc_offset + CRC_LEN])
        }
    }
}
