// This will be the main module for VortexWal, inspired by qdrant/wal/src/lib.rs

use crossbeam_channel::{Receiver, Sender};
use fs4::fs_std::FileExt;
use tracing::{debug, info, trace, warn, error}; // Use tracing macros
use std::cmp::Ordering;
// HashMap not used directly in this file's structs/methods from Qdrant's version
// use std::collections::HashMap; 
use std::fmt;
use std::fs::{self, File, OpenOptions}; // Added OpenOptions
use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;
use std::thread;

// Vortex-specific WAL components
pub mod segment; 
pub mod mmap_utils; 
pub mod wal_manager; // Declare wal_manager as a submodule of wal

use segment::{VortexEntry, VortexSegment}; 
// MmapViewSync is used within VortexSegment, not directly here.

#[derive(Debug, Clone)]
pub struct VortexWalOptions {
    pub segment_capacity: usize,
    pub segment_queue_len: usize,
    // Potential Vortex enhancements:
    // pub sync_on_every_write: bool,
    // pub sync_every_n_records: Option<usize>,
    // pub sync_every_t_ms: Option<u64>,
}

impl Default for VortexWalOptions {
    fn default() -> Self {
        VortexWalOptions {
            segment_capacity: 32 * 1024 * 1024, // 32MiB, same as Qdrant
            segment_queue_len: 0, // Qdrant default, consider 1 or 2 for Vortex
            // sync_on_every_write: false, // Default to performance over immediate sync
            // sync_every_n_records: None,
            // sync_every_t_ms: Some(1000), // e.g., sync every second by default if not on every write
        }
    }
}

#[derive(Debug)]
struct OpenVortexSegment {
    pub id: u64, // Segment ID
    pub segment: VortexSegment,
    pub start_lsn: u64, // Global LSN of the first entry this segment would hold if it has entries
}

#[derive(Debug)]
struct ClosedVortexSegment {
    pub start_index: u64, // First LSN in this segment
    pub segment: VortexSegment,
}

enum WalVortexSegment {
    Open(OpenVortexSegment),
    Closed(ClosedVortexSegment),
}

pub struct VortexWal {
    open_segment: OpenVortexSegment,
    closed_segments: Vec<ClosedVortexSegment>,
    creator: VortexSegmentCreator,
    #[allow(dead_code)] // dir is used for file lock
    dir_lock_file: File, // File handle for directory lock
    path: PathBuf, // Directory path for WAL segments
    // flush_handle is removed as VortexSegment::flush_async now returns Result<()>
    // and initiates an OS-level async flush.
    options: VortexWalOptions, // Store options
}

impl VortexWal {
    pub fn open<P>(path: P, options: VortexWalOptions) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        debug!("VortexWal {{ path: {:?} }}: opening with options {:?}", path.as_ref(), options);

        let wal_dir_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&wal_dir_path)?; // Ensure WAL directory exists

        let lock_file_path = wal_dir_path.join(".lock");
        let dir_lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_file_path)?;
        dir_lock_file.try_lock_exclusive()?;

        let mut parsed_open_segments_from_disk: Vec<OpenVortexSegment> = Vec::new();
        let mut closed_segments: Vec<ClosedVortexSegment> = Vec::new();

        for entry_res in fs::read_dir(&wal_dir_path)? {
            let entry = entry_res?;
            match Self::open_dir_entry(&entry)? {
                Some(WalVortexSegment::Open(open_segment)) => parsed_open_segments_from_disk.push(open_segment),
                Some(WalVortexSegment::Closed(closed_segment)) => closed_segments.push(closed_segment),
                None => {}
            }
        }

        closed_segments.sort_by(|a, b| a.start_index.cmp(&b.start_index));
        let mut next_expected_lsn = closed_segments.first().map_or(0, |cs| cs.start_index);

        for cs in &closed_segments {
            match cs.start_index.cmp(&next_expected_lsn) {
                Ordering::Less => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Overlapping WAL segments: expected LSN {}, found {} for segment {:?}", next_expected_lsn, cs.start_index, cs.segment.path()),
                    ));
                }
                Ordering::Equal => {
                    next_expected_lsn = cs.start_index + cs.segment.len() as u64;
                }
                Ordering::Greater => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Missing WAL segment(s): expected LSN {}, found {} for segment {:?}", next_expected_lsn, cs.start_index, cs.segment.path()),
                    ));
                }
            }
        }
        
        parsed_open_segments_from_disk.sort_by(|a, b| a.id.cmp(&b.id));
        let mut current_open_segment_candidate: Option<OpenVortexSegment> = None;
        let mut unused_open_segments: Vec<OpenVortexSegment> = Vec::new();

        for segment_candidate in parsed_open_segments_from_disk {
            if !segment_candidate.segment.is_empty() { // Current candidate is NOT empty
                if let Some(prev_candidate) = current_open_segment_candidate.take() {
                    if !prev_candidate.segment.is_empty() { // Previous candidate was also NOT empty
                        // Retire the older non-empty segment
                        warn!("Found multiple non-empty open segments. Retiring older non-empty one: {:?}", prev_candidate.segment.path());
                        let closed = Self::close_segment_static(prev_candidate, next_expected_lsn)?;
                        next_expected_lsn += closed.segment.len() as u64;
                        closed_segments.push(closed);
                    } else { // Previous candidate was empty
                        debug!("Found empty open segment {:?} before non-empty {:?}, adding former to unused.", prev_candidate.segment.path(), segment_candidate.segment.path());
                        unused_open_segments.push(prev_candidate);
                    }
                }
                current_open_segment_candidate = Some(segment_candidate);
            } else { // Current candidate IS empty
                if current_open_segment_candidate.is_none() {
                    // This is the first open segment encountered, and it's empty
                    current_open_segment_candidate = Some(segment_candidate);
                } else {
                    // There's already a current_open_segment_candidate (could be empty or not).
                    // This new segment_candidate is empty. Add it to unused.
                    // The existing current_open_segment_candidate (if empty and older) will be replaced if a non-empty one comes later,
                    // or it will remain if all subsequent ones are also empty (they'd go to unused).
                    debug!("Found empty open segment {:?} while already having candidate {:?}, adding former to unused.", segment_candidate.segment.path(), current_open_segment_candidate.as_ref().unwrap().segment.path());
                    unused_open_segments.push(segment_candidate);
                }
            }
        }
        
        let mut segment_creator = VortexSegmentCreator::new(
            &wal_dir_path,
            unused_open_segments,
            options.segment_capacity,
            options.segment_queue_len,
        );

        let final_open_segment = match current_open_segment_candidate {
            Some(mut os) => {
                os.start_lsn = next_expected_lsn; 
                os
            }
            None => {
                let mut new_seg = segment_creator.next()?;
                new_seg.start_lsn = next_expected_lsn; 
                new_seg
            }
        };

        let wal = Self {
            open_segment: final_open_segment,
            closed_segments,
            creator: segment_creator,
            dir_lock_file,
            path: wal_dir_path,
            // flush_handle: None, // Removed
            options,
        };
        info!("{:?}: opened successfully", wal);
        Ok(wal)
    }

    fn open_dir_entry(entry: &fs::DirEntry) -> Result<Option<WalVortexSegment>> {
        let path = entry.path();
        if !path.is_file() { return Ok(None); }

        let filename_os = entry.file_name();
        let filename = filename_os.to_str().ok_or_else(|| Error::new(ErrorKind::InvalidData, "Segment filename is not valid UTF-8"))?;

        match filename.split_once('-') {
            Some(("tmp", _)) => {
                warn!("Found temporary WAL segment file {:?}, removing.", path);
                fs::remove_file(&path)?;
                Ok(None)
            }
            Some(("open", id_str)) => {
                let id = u64::from_str(id_str).map_err(|_| Error::new(ErrorKind::InvalidData, format!("Invalid open segment ID: {}", id_str)))?;
                let segment_data = VortexSegment::open(&path)?;
                Ok(Some(WalVortexSegment::Open(OpenVortexSegment { segment: segment_data, id, start_lsn: 0 }))) // Placeholder start_lsn
            }
            Some(("closed", start_lsn_str)) => {
                let start_index = u64::from_str(start_lsn_str).map_err(|_| Error::new(ErrorKind::InvalidData, format!("Invalid closed segment start LSN: {}", start_lsn_str)))?;
                let segment = VortexSegment::open(&path)?;
                Ok(Some(WalVortexSegment::Closed(ClosedVortexSegment { start_index, segment })))
            }
            _ => {
                warn!("Ignoring unrecognized file in WAL directory: {:?}", path);
                Ok(None)
            }
        }
    }
    
    fn close_segment_static(mut open_segment_to_close: OpenVortexSegment, _new_start_lsn_for_closed: u64) -> Result<ClosedVortexSegment> {
        // The name of the closed segment file uses the actual start LSN of that segment's data.
        let filename_start_lsn = open_segment_to_close.start_lsn;
        let new_filename = format!("closed-{}", filename_start_lsn);
        let new_path = open_segment_to_close.segment.path().with_file_name(new_filename);
        open_segment_to_close.segment.rename(new_path)?;
        Ok(ClosedVortexSegment {
            start_index: filename_start_lsn, // This is the key: use the actual start_lsn of the segment being closed
            segment: open_segment_to_close.segment,
        })
    }

    async fn retire_open_segment(&mut self) -> Result<()> {
        trace!("{:?}: retiring open segment", self);
        
        let old_open_segment_start_lsn = self.open_segment.start_lsn;
        let old_open_segment_len = self.open_segment.segment.len() as u64;
        let next_open_segment_start_lsn = old_open_segment_start_lsn + old_open_segment_len;

        let mut incoming_open_segment = self.creator.next()?; 
        incoming_open_segment.start_lsn = next_open_segment_start_lsn;
        
        // Initiate async flush for the segment that is about to be closed.
        // This should be done *before* it's moved into segment_to_be_closed if flush_async needs &mut.
        // self.open_segment.segment is the one to be flushed.
        if let Err(e) = self.open_segment.segment.flush_async().await {
            // Log the error but proceed with retirement. The OS might still attempt the flush.
            // Or, depending on error severity, one might choose to handle it differently.
            // For now, log and continue, as per the original note's intent.
            error!("Error initiating async flush for segment {:?} during retirement: {:?}", self.open_segment.segment.path(), e);
        }
        // Note: If flush_async fails, we might want to handle the error, but for now,
        // we proceed with retirement. The OS will attempt the flush.

        let segment_to_be_closed = mem::replace(&mut self.open_segment, incoming_open_segment);
        let start_lsn_of_segment_to_close = segment_to_be_closed.start_lsn; // Extract LSN before move
        
        // The old flush_handle logic is removed as flush_async is now fire-and-forget OS level.
        // No explicit joining of a thread handle is needed here.
        
        if let Some(last_closed) = self.closed_segments.last() {
            if last_closed.segment.is_empty() {
                let empty_segment_to_delete = self.closed_segments.pop().unwrap();
                debug!("Deleting empty closed segment: {:?}", empty_segment_to_delete.segment.path());
                empty_segment_to_delete.segment.delete()?;
            }
        }
        
        // segment_to_be_closed.start_lsn is already correct.
        let closed_segment = Self::close_segment_static(segment_to_be_closed, start_lsn_of_segment_to_close)?;
        self.closed_segments.push(closed_segment);
        debug!("{:?}: open segment retired. New open segment starts at LSN: {}", self, self.open_segment.start_lsn);
        Ok(())
    }

    pub async fn append_bytes(&mut self, record_bytes: &[u8]) -> Result<u64> {
        trace!("{:?}: appending entry of length {}", self, record_bytes.len());
        if !self.open_segment.segment.sufficient_capacity(record_bytes.len()) {
            if !self.open_segment.segment.is_empty() { 
                self.retire_open_segment().await?;
            }
            self.open_segment.segment.ensure_capacity(record_bytes.len())?;
        }

        let ordinal_in_segment = self.open_segment.segment.append_record_bytes(record_bytes)
            .ok_or_else(|| Error::new(ErrorKind::StorageFull, "Failed to append to segment after ensuring capacity"))?;
        
        Ok(self.open_segment.start_lsn + ordinal_in_segment as u64)
    }
    
    pub fn read_bytes_by_lsn(&self, lsn: u64) -> Option<VortexEntry> {
        if lsn >= self.open_segment.start_lsn {
            let ordinal_in_segment = (lsn - self.open_segment.start_lsn) as usize;
            if ordinal_in_segment < self.open_segment.segment.len() {
                 return self.open_segment.segment.entry(ordinal_in_segment);
            }
        }

        match self.find_closed_segment_for_lsn(lsn) {
            Ok(segment_vec_idx) => {
                let closed_segment_info = &self.closed_segments[segment_vec_idx];
                let ordinal_in_segment = (lsn - closed_segment_info.start_index) as usize;
                closed_segment_info.segment.entry(ordinal_in_segment)
            }
            Err(_) => None, 
        }
    }

    pub fn truncate_log_from_lsn(&mut self, from_lsn: u64) -> Result<()> {
        trace!("{:?}: truncate from LSN {}", self, from_lsn);
        
        if from_lsn >= self.open_segment.start_lsn {
             // Check if from_lsn is within the current open segment's actual entries
            if (from_lsn - self.open_segment.start_lsn) < self.open_segment.segment.len() as u64 {
                let ordinal_in_segment = (from_lsn - self.open_segment.start_lsn) as usize;
                self.open_segment.segment.truncate_from_ordinal(ordinal_in_segment);
            } // If from_lsn is beyond current entries in open_segment, do nothing to open_segment
        } else { // Truncation point is before the open segment or at its start
            self.open_segment.segment.truncate_from_ordinal(0); 

            match self.find_closed_segment_for_lsn(from_lsn) {
                Ok(vec_idx) => {
                    let target_closed_segment = &mut self.closed_segments[vec_idx];
                    if from_lsn == target_closed_segment.start_index { 
                        for segment_to_delete in self.closed_segments.drain(vec_idx..) {
                            segment_to_delete.segment.delete()?;
                        }
                    } else {
                        let ordinal_in_segment = (from_lsn - target_closed_segment.start_index) as usize;
                        target_closed_segment.segment.truncate_from_ordinal(ordinal_in_segment);
                        target_closed_segment.segment.flush()?; 

                        if vec_idx + 1 < self.closed_segments.len() {
                            for segment_to_delete in self.closed_segments.drain(vec_idx + 1..) {
                                segment_to_delete.segment.delete()?;
                            }
                        }
                    }
                }
                Err(insertion_point) => { 
                    if from_lsn <= self.closed_segments.get(insertion_point).map_or(self.open_segment.start_lsn, |s| s.start_index) {
                        for segment_to_delete in self.closed_segments.drain(..) {
                            segment_to_delete.segment.delete()?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn prefix_truncate_log_until_lsn(&mut self, until_lsn: u64) -> Result<()> {
        trace!("{:?}: prefix_truncate until LSN {}", self, until_lsn);
        if self.open_segment.segment.is_empty() && self.closed_segments.is_empty() {
            return Ok(()); // Nothing to truncate
        }
        
        if until_lsn <= self.first_lsn().unwrap_or(0) { // If until is before or at the very start, do nothing
            return Ok(());
        }

        // If until_lsn is at or after the start of the open segment, all closed segments are eligible for deletion.
        if until_lsn >= self.open_segment.start_lsn {
            for segment_to_delete in self.closed_segments.drain(..) {
                debug!("Prefix truncating (until_lsn >= open_segment.start_lsn): deleting closed segment {:?}", segment_to_delete.segment.path());
                segment_to_delete.segment.delete()?;
            }
            // Note: We do not truncate the open segment itself in prefix_truncate.
            // If the open segment now effectively starts the WAL, its start_lsn remains its global start.
            return Ok(());
        }

        // Otherwise, until_lsn falls within the closed segments.
        // Delete all closed segments that end *before* until_lsn.
        // A segment `cs` (start_index, len) ends at `start_index + len - 1`.
        // We delete it if `start_index + len <= until_lsn`.
        // Or, more simply, delete up to the segment that *contains* or *is after* until_lsn.
        
        let mut first_retained_closed_idx = 0;
        for (idx, cs) in self.closed_segments.iter().enumerate() {
            if cs.start_index + cs.segment.len() as u64 > until_lsn { // This segment (or part of it) should be kept
                first_retained_closed_idx = idx;
                break;
            }
            // If loop finishes, all closed segments end before until_lsn
            first_retained_closed_idx = self.closed_segments.len(); 
        }

        for segment_to_delete in self.closed_segments.drain(..first_retained_closed_idx) {
            segment_to_delete.segment.delete()?;
        }
        Ok(())
    }
    
    fn find_closed_segment_for_lsn(&self, lsn: u64) -> result::Result<usize, usize> {
        self.closed_segments.binary_search_by(|cs| {
            if lsn < cs.start_index { Ordering::Greater }
            else if lsn >= cs.start_index + cs.segment.len() as u64 { Ordering::Less }
            else { Ordering::Equal }
        })
    }

    pub fn first_lsn(&self) -> Option<u64> {
        if !self.closed_segments.is_empty() {
            self.closed_segments.first().map(|cs| cs.start_index)
        } else if !self.open_segment.segment.is_empty() {
            Some(self.open_segment.start_lsn)
        } else {
            None
        }
    }

    pub fn last_lsn(&self) -> Option<u64> {
        if !self.open_segment.segment.is_empty() {
            Some(self.open_segment.start_lsn + self.open_segment.segment.len() as u64 - 1)
        } else if let Some(last_closed) = self.closed_segments.last() {
            if !last_closed.segment.is_empty() {
                 Some(last_closed.start_index + last_closed.segment.len() as u64 - 1)
            } else if self.closed_segments.len() > 1 { // Last closed is empty, check one before it
                 let prev_closed = &self.closed_segments[self.closed_segments.len()-2];
                 if !prev_closed.segment.is_empty() {
                    Some(prev_closed.start_index + prev_closed.segment.len() as u64 -1)
                 } else { None } // Should not happen if empty segments are pruned correctly
            } else { None } // Only one closed segment and it's empty
        } else {
            None // WAL is empty
        }
    }
}

impl fmt::Debug for VortexWal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VortexWal {{ path: {:?}, options: {:?}, num_closed_segments: {}, open_segment_id: {}, open_segment_start_lsn: {}, first_lsn: {:?}, last_lsn: {:?} }}",
            &self.path,
            &self.options,
            self.closed_segments.len(),
            self.open_segment.id,
            self.open_segment.start_lsn, // Added for clarity
            self.first_lsn(),
            self.last_lsn()
        )
    }
}

struct VortexSegmentCreator {
    rx: Option<Receiver<OpenVortexSegment>>,
    thread: Option<thread::JoinHandle<Result<()>>>,
}

impl VortexSegmentCreator {
    pub fn new<P>(
        dir: P,
        existing: Vec<OpenVortexSegment>,
        segment_capacity: usize,
        segment_queue_len: usize,
    ) -> Self
    where
        P: AsRef<Path>,
    {
        let (tx, rx) = crossbeam_channel::bounded(segment_queue_len.max(1)); 
        let dir_owned = dir.as_ref().to_path_buf();
        let thread = thread::spawn(move || {
            Self::creation_loop(tx, dir_owned, segment_capacity, existing)
        });
        Self { rx: Some(rx), thread: Some(thread) }
    }

    pub fn next(&mut self) -> Result<OpenVortexSegment> {
        self.rx.as_ref().expect("SegmentCreator rx channel is None").recv().map_err(|_| {
            match self.thread.take().map(|jh| jh.join()) {
                Some(Ok(Err(e))) => e, 
                Some(Err(panic_payload)) => Error::new(ErrorKind::Other, format!("Segment creator thread panicked: {:?}", panic_payload)),
                None => Error::new(ErrorKind::Other, "Segment creator thread already joined/failed"),
                Some(Ok(Ok(()))) => Error::new(ErrorKind::Other, "Segment creator channel closed but thread finished successfully (unexpected)"),
            }
        })
    }

    fn creation_loop(
        tx: Sender<OpenVortexSegment>,
        base_path: PathBuf, 
        capacity: usize,
        mut existing_segments: Vec<OpenVortexSegment>,
    ) -> Result<()> {
        existing_segments.sort_by(|a, b| a.id.cmp(&b.id));
        let mut next_segment_id = existing_segments.last().map_or(0, |s| s.id) + 1;

        for segment_to_send in existing_segments { 
            if tx.send(segment_to_send).is_err() { 
                return Ok(()); 
            }
        }
        
        #[cfg(not(target_os = "windows"))]
        let dir_sync_file = File::open(&base_path)?;

        loop {
            let segment_file_path = base_path.join(format!("open-{}", next_segment_id));
            let new_segment_data = match VortexSegment::create(&segment_file_path, capacity) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to create new WAL segment {:?}: {}", segment_file_path, e);
                    return Err(e); 
                }
            };
            let new_open_segment = OpenVortexSegment { 
                id: next_segment_id, 
                segment: new_segment_data, 
                start_lsn: 0 // Placeholder, will be set by VortexWal
            };
            
            #[cfg(not(target_os = "windows"))]
            dir_sync_file.sync_all()?; 

            if tx.send(new_open_segment).is_err() {
                info!("SegmentCreator shutting down as receiver channel closed.");
                if let Err(e) = fs::remove_file(&segment_file_path) {
                     warn!("Failed to remove unused pre-created segment {:?}: {}", segment_file_path, e);
                }
                return Ok(());
            }
            next_segment_id += 1;
        }
    }
}

impl Drop for VortexSegmentCreator {
    fn drop(&mut self) {
        drop(self.rx.take()); 
        if let Some(handle) = self.thread.take() {
            if let Err(e) = handle.join() {
                warn!("SegmentCreator thread panicked on drop: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::io::Write; // Added for file.write_all and file.flush

    async fn create_test_wal_with_options(options: VortexWalOptions) -> (tempfile::TempDir, VortexWal) { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let wal = VortexWal::open(dir.path(), options).expect("Failed to open WAL");
        (dir, wal)
    }

    fn default_test_options(segment_capacity: usize) -> VortexWalOptions {
        VortexWalOptions {
            segment_capacity,
            segment_queue_len: 0, 
        }
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_new_empty() { // Made async
        let (_dir, wal) = create_test_wal_with_options(default_test_options(1024)).await; // Added .await
        assert!(wal.first_lsn().is_none(), "First LSN should be None for new WAL");
        assert!(wal.last_lsn().is_none(), "Last LSN should be None for new WAL");
        assert_eq!(wal.open_segment.segment.len(), 0, "Open segment should be empty");
        assert_eq!(wal.closed_segments.len(), 0, "Should be no closed segments");
        assert_eq!(wal.open_segment.start_lsn, 0, "Open segment of new WAL should start at LSN 0");
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_append_single_entry() { // Made async
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(1024)).await; // Added .await
        let entry_data = b"test_entry_1";
        let lsn = wal.append_bytes(entry_data).await.expect("Append failed"); // Added .await
        
        assert_eq!(lsn, 0, "LSN of first entry should be 0");
        assert_eq!(wal.first_lsn(), Some(0));
        assert_eq!(wal.last_lsn(), Some(0));
        assert_eq!(wal.open_segment.segment.len(), 1, "Open segment should have 1 entry");
        assert_eq!(wal.open_segment.start_lsn, 0);

        let read_entry = wal.read_bytes_by_lsn(0).expect("Read failed for LSN 0");
        assert_eq!(&*read_entry, entry_data, "Read data mismatch");
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_append_multiple_entries_single_segment() { // Made async
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(1024)).await; // Added .await
        let entry1 = b"entry_A";
        let entry2 = b"entry_BB";
        let entry3 = b"entry_CCC";

        wal.append_bytes(entry1).await.unwrap(); // Added .await
        wal.append_bytes(entry2).await.unwrap(); // Added .await
        wal.append_bytes(entry3).await.unwrap(); // Added .await

        assert_eq!(wal.first_lsn(), Some(0));
        assert_eq!(wal.last_lsn(), Some(2));
        assert_eq!(wal.open_segment.segment.len(), 3);
        assert_eq!(wal.open_segment.start_lsn, 0);
        assert_eq!(wal.closed_segments.len(), 0);

        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), entry1);
        assert_eq!(&*wal.read_bytes_by_lsn(1).unwrap(), entry2);
        assert_eq!(&*wal.read_bytes_by_lsn(2).unwrap(), entry3);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_reopen_empty() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        {
            // create_test_wal_with_options is now async, so we need to await it or open directly
            let _wal = VortexWal::open(dir.path(), options.clone()).expect("Failed to open WAL first time");
        }
        
        let wal2 = VortexWal::open(dir.path(), options).expect("Failed to reopen WAL");
        assert!(wal2.first_lsn().is_none());
        assert!(wal2.last_lsn().is_none());
        assert_eq!(wal2.open_segment.segment.len(), 0);
        assert_eq!(wal2.open_segment.start_lsn, 0);
        assert_eq!(wal2.closed_segments.len(), 0);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_reopen_with_data_in_open_segment() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        let entry1 = b"reopen_data_1";
        let entry2 = b"reopen_data_2";

        {
            let mut wal = VortexWal::open(dir.path(), options.clone()).expect("Failed to open WAL first time");
            wal.append_bytes(entry1).await.unwrap(); // Added .await
            wal.append_bytes(entry2).await.unwrap(); // Added .await
        }

        let mut wal2 = VortexWal::open(dir.path(), options).expect("Failed to reopen WAL");
        assert_eq!(wal2.first_lsn(), Some(0));
        assert_eq!(wal2.last_lsn(), Some(1));
        assert_eq!(wal2.open_segment.segment.len(), 2);
        assert_eq!(wal2.open_segment.start_lsn, 0);
        assert_eq!(wal2.closed_segments.len(), 0);
        
        assert_eq!(&*wal2.read_bytes_by_lsn(0).unwrap(), entry1);
        assert_eq!(&*wal2.read_bytes_by_lsn(1).unwrap(), entry2);

        let entry3 = b"reopen_data_3";
        let lsn3 = wal2.append_bytes(entry3).await.unwrap(); // Added .await
        assert_eq!(lsn3, 2);
        assert_eq!(wal2.last_lsn(), Some(2));
        assert_eq!(wal2.open_segment.segment.len(), 3);
        assert_eq!(wal2.open_segment.start_lsn, 0);
        assert_eq!(&*wal2.read_bytes_by_lsn(2).unwrap(), entry3);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_segment_retirement() { // Made async
        let segment_capacity = 64; 
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        let entry_data = b"0123456789"; 
        let record_size = 8 + entry_data.len() + segment::padding(entry_data.len()) + 4; 
        assert!(segment_capacity >= 2 * record_size && segment_capacity < 3 * record_size);

        wal.append_bytes(entry_data).await.unwrap(); // Added .await
        wal.append_bytes(entry_data).await.unwrap(); // Added .await
        assert_eq!(wal.open_segment.start_lsn, 0);
        assert_eq!(wal.open_segment.segment.len(), 2);

        wal.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 2, retires (0,1)
        assert_eq!(wal.closed_segments.len(), 1);
        assert_eq!(wal.closed_segments[0].start_index, 0);
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        assert_eq!(wal.open_segment.start_lsn, 2);
        assert_eq!(wal.open_segment.segment.len(), 1);
        assert_eq!(wal.first_lsn(), Some(0));
        assert_eq!(wal.last_lsn(), Some(2));
        
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), entry_data);
        assert_eq!(&*wal.read_bytes_by_lsn(1).unwrap(), entry_data);
        assert_eq!(&*wal.read_bytes_by_lsn(2).unwrap(), entry_data);

        wal.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 3
        assert_eq!(wal.open_segment.start_lsn, 2);
        assert_eq!(wal.open_segment.segment.len(), 2);

        wal.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 4, retires (2,3)
        assert_eq!(wal.closed_segments.len(), 2);
        assert_eq!(wal.closed_segments[1].start_index, 2);
        assert_eq!(wal.closed_segments[1].segment.len(), 2);
        assert_eq!(wal.open_segment.start_lsn, 4);
        assert_eq!(wal.open_segment.segment.len(), 1);
        assert_eq!(wal.first_lsn(), Some(0));
        assert_eq!(wal.last_lsn(), Some(4));
        
        assert_eq!(&*wal.read_bytes_by_lsn(3).unwrap(), entry_data);
        assert_eq!(&*wal.read_bytes_by_lsn(4).unwrap(), entry_data);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_reopen_with_closed_segments() { // Made async
        let segment_capacity = 64;
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(segment_capacity);
        let entry_data = b"0123456789"; 

        {
            let mut wal = VortexWal::open(dir.path(), options.clone()).unwrap();
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
        }

        let mut wal2 = VortexWal::open(dir.path(), options).unwrap();
        assert_eq!(wal2.closed_segments.len(), 1);
        assert_eq!(wal2.closed_segments[0].start_index, 0);
        assert_eq!(wal2.closed_segments[0].segment.len(), 2);
        
        assert_eq!(wal2.open_segment.segment.len(), 2);
        assert_eq!(wal2.open_segment.start_lsn, 2); // Key check for reopen

        assert_eq!(wal2.first_lsn(), Some(0));
        assert_eq!(wal2.last_lsn(), Some(3));

        assert_eq!(&*wal2.read_bytes_by_lsn(0).unwrap(), entry_data);
        assert_eq!(&*wal2.read_bytes_by_lsn(1).unwrap(), entry_data);
        assert_eq!(&*wal2.read_bytes_by_lsn(2).unwrap(), entry_data);
        assert_eq!(&*wal2.read_bytes_by_lsn(3).unwrap(), entry_data);

        let lsn4 = wal2.append_bytes(entry_data).await.unwrap(); // Added .await
        assert_eq!(lsn4, 4);
        assert_eq!(wal2.closed_segments.len(), 2);
        assert_eq!(wal2.closed_segments[1].start_index, 2);
        assert_eq!(wal2.closed_segments[1].segment.len(), 2); 
        assert_eq!(wal2.open_segment.segment.len(), 1); 
        assert_eq!(wal2.open_segment.start_lsn, 4);
        assert_eq!(wal2.last_lsn(), Some(4));
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_truncate_from_lsn() { // Made async
        let segment_capacity = 64; 
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        let entry_data = b"0123456789"; 

        for _i in 0..5 { // LSN 0..4. closed[0]=(0,1), closed[1]=(2,3), open=(4, start_lsn=4)
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
        }
        assert_eq!(wal.open_segment.start_lsn, 4);

        wal.truncate_log_from_lsn(4).unwrap(); 
        assert_eq!(wal.last_lsn(), Some(3));
        assert_eq!(wal.open_segment.segment.len(), 0);
        assert_eq!(wal.open_segment.start_lsn, 4); // Start LSN of open segment doesn't change by truncating its content
        assert_eq!(wal.closed_segments.len(), 2); 
        assert!(wal.read_bytes_by_lsn(3).is_some());

        wal.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 4 again. open_segment=(4, start_lsn=4)
        assert_eq!(wal.last_lsn(), Some(4));
        assert_eq!(wal.open_segment.start_lsn, 4);

        wal.truncate_log_from_lsn(3).unwrap(); // Truncate LSN 3 onwards. open=(empty, start_lsn=4), closed[1]=(2, start_lsn=2)
        assert_eq!(wal.last_lsn(), Some(2));
        assert_eq!(wal.open_segment.segment.len(), 0);
        assert_eq!(wal.open_segment.start_lsn, 4); // start_lsn of open segment itself is fixed until retired
        assert_eq!(wal.closed_segments.len(), 2); 
        assert_eq!(wal.closed_segments[1].segment.len(), 1);
        assert_eq!(wal.closed_segments[1].start_index, 2);
        assert!(wal.read_bytes_by_lsn(2).is_some());
        
        let (_dir2, mut wal2) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _i in 0..6 { // LSN 0..5. closed[0]=(0,1), closed[1]=(2,3), open=(4,5, start_lsn=4)
            wal2.append_bytes(entry_data).await.unwrap(); // Added .await
        }
        wal2.truncate_log_from_lsn(2).unwrap(); 
        assert_eq!(wal2.last_lsn(), Some(1));
        assert_eq!(wal2.open_segment.segment.len(), 0); 
        assert_eq!(wal2.open_segment.start_lsn, 4); // Open segment was (4,5), now empty, but its designated start was 4.
        assert_eq!(wal2.closed_segments.len(), 1); 
        assert_eq!(wal2.closed_segments[0].start_index, 0);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_prefix_truncate_log_until_lsn() { // Made async
        let segment_capacity = 64; 
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        let entry_data = b"0123456789";

        for _i in 0..6 { // LSN 0..5. closed[0]=(0,1), closed[1]=(2,3), open=(4,5, start_lsn=4)
            wal.append_bytes(entry_data).await.unwrap(); // Added .await
        }
        
        wal.prefix_truncate_log_until_lsn(0).unwrap();
        assert_eq!(wal.closed_segments.len(), 2);
        assert_eq!(wal.first_lsn(), Some(0));

        wal.prefix_truncate_log_until_lsn(2).unwrap(); // Remove (0,1)
        assert_eq!(wal.closed_segments.len(), 1);
        assert_eq!(wal.closed_segments[0].start_index, 2);
        assert_eq!(wal.first_lsn(), Some(2));
        
        wal.prefix_truncate_log_until_lsn(4).unwrap(); // Remove (2,3)
        assert_eq!(wal.closed_segments.len(), 0);
        assert_eq!(wal.open_segment.segment.len(), 2); // (4,5)
        assert_eq!(wal.open_segment.start_lsn, 4);
        assert_eq!(wal.first_lsn(), Some(4));
        assert_eq!(wal.last_lsn(), Some(5));
        
        wal.append_bytes(entry_data).await.unwrap(); // Added .await
        wal.append_bytes(entry_data).await.unwrap(); // Added .await
        wal.append_bytes(entry_data).await.unwrap(); // Added .await
        
        assert_eq!(wal.closed_segments.len(), 2); // (4,5) and (6,7)
        assert_eq!(wal.closed_segments[0].start_index, 4);
        assert_eq!(wal.closed_segments[1].start_index, 6);
        assert_eq!(wal.open_segment.start_lsn, 8);
        assert_eq!(wal.first_lsn(), Some(4));
        assert_eq!(wal.last_lsn(), Some(8));

        wal.prefix_truncate_log_until_lsn(10).unwrap(); 
        assert_eq!(wal.closed_segments.len(), 0);
        assert_eq!(wal.open_segment.segment.len(), 1); 
        assert_eq!(wal.open_segment.start_lsn, 8);
        assert_eq!(wal.first_lsn(), Some(8));
        assert_eq!(wal.last_lsn(), Some(8));
    }

    // TODO: Add more comprehensive unit tests for VortexWal functionality,
    // focusing on edge cases and complex sequences of operations.
    // - Error handling in open() (corrupted files, overlapping/missing segments)
    // - append_bytes() edge cases (entry exact size of capacity, entry larger than capacity)
    // - read_bytes_by_lsn() edge cases (out of bounds, empty WAL)
    // - truncate_log_from_lsn() complex scenarios (truncate at 0, first_lsn, last_lsn, middle of closed, make all empty)
    // - prefix_truncate_log_until_lsn() complex scenarios (until 0, first_lsn, last_lsn, delete all closed, beyond last_lsn)
    // - Interactions: append-truncate-append, append-prefix_truncate-append, multiple truncations.
    // - Reopening after complex sequences.

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_with_overlapping_closed_segments() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        let wal_path = dir.path();

        // Create segment closed-0 with 2 entries
        let seg0_path = wal_path.join("closed-0");
        let mut seg0 = VortexSegment::create(&seg0_path, 1024).unwrap();
        seg0.append_record_bytes(b"entry0").unwrap();
        seg0.append_record_bytes(b"entry1").unwrap();
        seg0.flush().unwrap();
        drop(seg0); // Ensure file is closed

        // Create segment closed-1 (overlaps with closed-0) with 1 entry
        let seg1_path = wal_path.join("closed-1");
        let mut seg1 = VortexSegment::create(&seg1_path, 1024).unwrap();
        seg1.append_record_bytes(b"entry_overlap").unwrap();
        seg1.flush().unwrap();
        drop(seg1);

        match VortexWal::open(wal_path, options) {
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::InvalidData);
                assert!(e.to_string().contains("Overlapping WAL segments"));
            }
            Ok(_) => panic!("WAL open should have failed due to overlapping segments"),
        }
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_with_missing_closed_segments() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        let wal_path = dir.path();

        // Create segment closed-0 with 2 entries
        let seg0_path = wal_path.join("closed-0");
        let mut seg0 = VortexSegment::create(&seg0_path, 1024).unwrap();
        seg0.append_record_bytes(b"entry0").unwrap();
        seg0.append_record_bytes(b"entry1").unwrap(); // Ends at LSN 1
        seg0.flush().unwrap();
        drop(seg0);

        // Create segment closed-3 (missing segment for LSN 2)
        let seg3_path = wal_path.join("closed-3");
        let mut seg3 = VortexSegment::create(&seg3_path, 1024).unwrap();
        seg3.append_record_bytes(b"entry3").unwrap();
        seg3.flush().unwrap();
        drop(seg3);

        match VortexWal::open(wal_path, options) {
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::InvalidData);
                assert!(e.to_string().contains("Missing WAL segment(s)"));
            }
            Ok(_) => panic!("WAL open should have failed due to missing segments"),
        }
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_with_multiple_non_empty_open_segments() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        let wal_path = dir.path();

        // Create open-0 with some data
        let open0_path = wal_path.join("open-0");
        let mut open0 = VortexSegment::create(&open0_path, 1024).unwrap();
        open0.append_record_bytes(b"open_data_0_a").unwrap();
        open0.append_record_bytes(b"open_data_0_b").unwrap();
        open0.flush().unwrap();
        drop(open0);

        // Create open-1 with some data (newer ID)
        let open1_path = wal_path.join("open-1");
        let mut open1 = VortexSegment::create(&open1_path, 1024).unwrap();
        open1.append_record_bytes(b"open_data_1_a").unwrap();
        open1.flush().unwrap();
        drop(open1);
        
        // Create open-2, empty (should be preferred if open-0 and open-1 are retired)
        let open2_path = wal_path.join("open-2");
        let open2 = VortexSegment::create(&open2_path, 1024).unwrap();
        drop(open2);


        let wal = VortexWal::open(wal_path, options).expect("WAL open failed");
        
        // Expect open-0 to be closed (start_lsn 0, len 2)
        // Expect open-1 to be the current open segment (start_lsn 2, len 1)
        // OR, if the logic prefers the highest ID non-empty as open, then open-1 is open.
        // The current logic: sorts by ID, if multiple non-empty, keeps the one with highest ID as open,
        // and attempts to close others. If an empty one with higher ID exists, it might be chosen.
        // Let's trace the logic:
        // parsed_open_segments_from_disk: [open-0 (len 2), open-1 (len 1), open-2 (len 0)]
        // Loop:
        //  - segment_candidate = open-0. current_open_segment_candidate = Some(open-0).
        //  - segment_candidate = open-1. prev_open = open-0. open-0 closed (start_lsn 0, len 2). next_expected_lsn = 2.
        //    current_open_segment_candidate = Some(open-1).
        //  - segment_candidate = open-2 (empty). current_open_segment_candidate is Some(open-1). unused_open_segments.push(open-2).
        // final_open_segment = open-1. open-1.start_lsn = next_expected_lsn (which is 2).
        
        assert_eq!(wal.closed_segments.len(), 1, "Should be one closed segment (from open-0)");
        assert_eq!(wal.closed_segments[0].start_index, 0);
        assert_eq!(wal.closed_segments[0].segment.len(), 2);
        
        assert_eq!(wal.open_segment.id, 1, "Open segment should be from open-1");
        assert_eq!(wal.open_segment.segment.len(), 1);
        assert_eq!(wal.open_segment.start_lsn, 2, "Open segment should start at LSN 2");
        
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), b"open_data_0_a");
        assert_eq!(&*wal.read_bytes_by_lsn(1).unwrap(), b"open_data_0_b");
        assert_eq!(&*wal.read_bytes_by_lsn(2).unwrap(), b"open_data_1_a");
        assert!(wal.read_bytes_by_lsn(3).is_none());

        // Check that segment creator has open-2 available
        let mut creator_path_check = wal.path.clone();
        creator_path_check.push("open-2");
        assert!(creator_path_check.exists(), "Pre-created segment open-2 should exist for creator");
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_prefers_non_empty_open_over_empty_with_lower_id() { // Made async
        let dir = tempdir().expect("Failed to create temp dir");
        let options = default_test_options(1024);
        let wal_path = dir.path();

        // Create open-0, empty
        let open0_path = wal_path.join("open-0");
        let open0 = VortexSegment::create(&open0_path, 1024).unwrap();
        drop(open0);

        // Create open-1 with some data
        let open1_path = wal_path.join("open-1");
        let mut open1 = VortexSegment::create(&open1_path, 1024).unwrap();
        open1.append_record_bytes(b"data_in_open_1").unwrap();
        open1.flush().unwrap();
        drop(open1);

        let wal = VortexWal::open(wal_path, options).expect("WAL open failed");

        // Logic:
        // parsed_open_segments_from_disk: [open-0 (empty), open-1 (len 1)]
        // Loop:
        //  - segment_candidate = open-0. current_open_segment_candidate = Some(open-0).
        //  - segment_candidate = open-1. prev_open = open-0. open-0 is empty.
        //    current_open_segment_candidate = Some(open-1). unused_open_segments.push(open-0)
        // final_open_segment = open-1. open-1.start_lsn = 0.

        assert_eq!(wal.closed_segments.len(), 0, "No closed segments expected");
        assert_eq!(wal.open_segment.id, 1, "Open segment should be from open-1");
        assert_eq!(wal.open_segment.segment.len(), 1);
        assert_eq!(wal.open_segment.start_lsn, 0, "Open segment should start at LSN 0");
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), b"data_in_open_1");
        
        // Check that segment creator has open-0 available
        let mut creator_path_check = wal.path.clone();
        creator_path_check.push("open-0");
        assert!(creator_path_check.exists(), "Pre-created segment open-0 should exist for creator");
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_append_fills_segment_then_retires() { // Made async
        let entry_data_small = b"small";
        let entry_data_large = vec![0u8; 100]; // An entry that takes up significant space
        
        // Calculate required capacity for large entry + small entry, such that large almost fills it.
        let large_entry_on_disk_size = VortexSegment::on_disk_size(entry_data_large.len());
        let small_entry_on_disk_size = VortexSegment::on_disk_size(entry_data_small.len());
        
        // Segment capacity such that large_entry fits, but large_entry + small_entry does not.
        let segment_capacity = large_entry_on_disk_size + small_entry_on_disk_size / 2; 
        // Ensure segment_capacity is at least large_entry_on_disk_size
        let segment_capacity = segment_capacity.max(large_entry_on_disk_size);


        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await

        // Append the large entry, should fit in the first segment.
        let lsn0 = wal.append_bytes(&entry_data_large).await.unwrap(); // Added .await
        assert_eq!(lsn0, 0);
        assert_eq!(wal.open_segment.segment.len(), 1);
        assert_eq!(wal.open_segment.start_lsn, 0);
        assert_eq!(wal.closed_segments.len(), 0);
        assert!(wal.open_segment.segment.capacity() >= large_entry_on_disk_size);


        // Append the small entry, should force retirement of the first segment.
        let lsn1 = wal.append_bytes(entry_data_small).await.unwrap(); // Added .await
        assert_eq!(lsn1, 1);
        
        assert_eq!(wal.closed_segments.len(), 1, "Segment should have been retired");
        assert_eq!(wal.closed_segments[0].start_index, 0);
        assert_eq!(wal.closed_segments[0].segment.len(), 1); // First segment had only the large entry
        
        assert_eq!(wal.open_segment.segment.len(), 1, "New open segment should have one entry");
        assert_eq!(wal.open_segment.start_lsn, 1, "New open segment should start at LSN 1");
        assert!(wal.open_segment.segment.capacity() >= small_entry_on_disk_size);


        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), &entry_data_large[..]);
        assert_eq!(&*wal.read_bytes_by_lsn(1).unwrap(), &entry_data_small[..]);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_append_very_large_entry() { // Made async
        // Options define initial segment capacity. Segment can grow if a single entry is larger.
        let initial_segment_capacity = 128; 
        let very_large_entry_data = vec![0u8; initial_segment_capacity * 2]; // Entry larger than initial capacity (256 bytes)
        let on_disk_size_very_large = VortexSegment::on_disk_size(very_large_entry_data.len());

        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(initial_segment_capacity)).await; // Added .await

        // Append the very large entry.
        // The initial open segment (empty, capacity 128) will call ensure_capacity and should resize.
        let lsn0 = wal.append_bytes(&very_large_entry_data).await.unwrap(); // Added .await
        assert_eq!(lsn0, 0);
        assert_eq!(wal.open_segment.segment.len(), 1, "Segment should contain the large entry");
        assert_eq!(wal.open_segment.start_lsn, 0, "Start LSN of open segment should be 0");
        assert_eq!(wal.closed_segments.len(), 0, "No closed segments yet");
        let large_segment_actual_capacity = wal.open_segment.segment.capacity();
        assert!(large_segment_actual_capacity >= on_disk_size_very_large, "Segment capacity should be at least size of large entry on disk");
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), &very_large_entry_data[..]);

        // Append another small entry. This should NOT retire the very large segment if it has space.
        let small_entry_data = b"small_after_large"; // len 17
        let _on_disk_size_small = VortexSegment::on_disk_size(small_entry_data.len());

        // Check if retirement is expected (it's not, based on trace)
        // Current open segment capacity is large_segment_actual_capacity.
        // Current size is on_disk_size_very_large.
        // If large_segment_actual_capacity >= on_disk_size_very_large + on_disk_size_small, no retirement.
        // e.g. on_disk_size_very_large (for 256 data) = 8+256+0+4 = 268.
        //      on_disk_size_small (for 17 data) = 8+17+7+4 = 36.
        //      large_segment_actual_capacity likely (268.next_power_of_two()) = 512 or ( (268).max(128*2) = 268.next_power_of_two() ) = 512
        //      So, 512 >= 268 + 36 (304). True. No retirement.

        let lsn1 = wal.append_bytes(small_entry_data).await.unwrap(); // Added .await
        assert_eq!(lsn1, 1, "LSN of small entry should be 1"); 

        // Assertions assuming NO retirement:
        assert_eq!(wal.closed_segments.len(), 0, "No segments should be closed after small append");
        
        assert_eq!(wal.open_segment.segment.len(), 2, "Open segment should now have two entries"); 
        assert_eq!(wal.open_segment.start_lsn, 0, "Open segment start LSN should still be 0");
        assert_eq!(wal.open_segment.segment.capacity(), large_segment_actual_capacity, "Open segment capacity should be unchanged (still the large one)"); 
                                                                                
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), &very_large_entry_data[..]);
        assert_eq!(&*wal.read_bytes_by_lsn(1).unwrap(), &small_entry_data[..]);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_read_bytes_edge_cases() { // Made async
        let (_dir, mut wal) = create_test_wal_with_options(default_test_options(1024)).await; // Added .await

        // 1. Read from empty WAL
        assert!(wal.read_bytes_by_lsn(0).is_none(), "Read from LSN 0 on empty WAL should be None");
        assert!(wal.read_bytes_by_lsn(100).is_none(), "Read from LSN 100 on empty WAL should be None");

        // Add some entries: LSN 0, 1, 2
        let entry0 = b"entry_zero";
        let entry1 = b"entry_one";
        let entry2 = b"entry_two";
        wal.append_bytes(entry0).await.unwrap(); // Added .await
        wal.append_bytes(entry1).await.unwrap(); // Added .await
        wal.append_bytes(entry2).await.unwrap(); // Added .await
        // WAL state: open_segment(start_lsn=0, len=3, entries=[0,1,2])

        // 2. Read LSN before first_lsn (not possible if first_lsn is 0, unless we prefix_truncate)
        // Let's test reading LSN < 0 effectively, e.g. if first_lsn becomes > 0
        // This is better tested after prefix_truncate, covered in a different test.
        // For now, with first_lsn = 0, any u64 < 0 is not possible.

        // 3. Read LSN after last_lsn
        assert!(wal.read_bytes_by_lsn(3).is_none(), "Read LSN after last_lsn should be None");
        assert!(wal.read_bytes_by_lsn(100).is_none(), "Read far LSN after last_lsn should be None");

        // 4. Reading from an empty open segment
        // Truncate all entries from open segment: LSN 0, 1, 2
        wal.truncate_log_from_lsn(0).unwrap();
        // WAL state: open_segment(start_lsn=0, len=0), closed_segments=[]
        assert!(wal.open_segment.segment.is_empty(), "Open segment should be empty after truncate from 0");
        assert_eq!(wal.open_segment.start_lsn, 0, "Open segment start_lsn should remain 0");
        assert!(wal.read_bytes_by_lsn(0).is_none(), "Read from LSN 0 on empty open segment should be None");
        assert!(wal.read_bytes_by_lsn(1).is_none(), "Read from LSN 1 on empty open segment should be None");
        
        // Add entries again to create closed segments
        // Segment capacity 64, entry "0123456789" (10 bytes data)
        // On disk: 8 (hdr) + 10 (data) + 6 (pad) + 4 (crc) = 28 bytes. 64/28 = 2 entries per segment.
        let (_dir2, mut wal2) = create_test_wal_with_options(default_test_options(64)).await; // Added .await
        let entry_data = b"0123456789";
        wal2.append_bytes(entry_data).await.unwrap(); // Added .await
        wal2.append_bytes(entry_data).await.unwrap(); // Added .await
        wal2.append_bytes(entry_data).await.unwrap(); // Added .await
        // WAL2 state: closed[0]=(start=0, len=2), open=(start=2, len=1, entry=[2])
        
        // 5. Reading from an empty closed segment (if possible to create)
        // Current logic in retire_open_segment prunes empty closed segments if they are last.
        // To test reading from an empty closed segment, it must not be the last one,
        // or we need to manually create such a state (harder in unit test without file manipulation).
        // Let's try to create an empty closed segment by truncating all its entries.
        // Truncate closed_segments[0] entirely by truncating from LSN 0.
        wal2.truncate_log_from_lsn(0).unwrap();
        // WAL2 state after truncate_from_lsn(0):
        // - open_segment.truncate_from_ordinal(0) -> open_segment is now empty (start=2, len=0)
        // - find_closed_segment_for_lsn(0) -> Ok(0) (finds closed[0])
        // - from_lsn (0) == target_closed_segment.start_index (0) -> true
        // - closed_segments.drain(0..) -> all closed segments deleted.
        // So, closed_segments is now empty. open_segment is (start=2, len=0).
        assert_eq!(wal2.closed_segments.len(), 0, "All closed segments should be deleted");
        assert!(wal2.open_segment.segment.is_empty(), "Open segment should be empty");
        assert_eq!(wal2.open_segment.start_lsn, 2);
        assert!(wal2.read_bytes_by_lsn(0).is_none(), "Read from LSN 0 (originally in deleted closed segment) should be None");
        assert!(wal2.read_bytes_by_lsn(1).is_none(), "Read from LSN 1 (originally in deleted closed segment) should be None");

        // Scenario: create an empty closed segment that is NOT last.
        // Requires careful setup.
        // Seg1 (0,1), Seg2 (empty, start 2), Seg3 (3,4)
        // This state is hard to achieve naturally because retire_open_segment creates non-empty closed segments,
        // and empty open segments are only retired if they are followed by non-empty ones.
        // The logic `if last_closed.segment.is_empty()` in `retire_open_segment`
        // also means an empty closed segment would be deleted if it's the last one when a retirement happens.
        // So, a persisted empty closed segment that is not last is unlikely.
        // If VortexWal::open encounters an empty closed segment file, it should load it.
        // Let's defer testing reading from an empty *closed* segment that's not last, as it's a very specific state.
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_truncate_complex_scenarios() { // Made async
        let entry_data = b"0123456789"; // Approx 28 bytes on disk per entry
        let segment_capacity = 64; // Fits 2 entries

        // Scenario 1: Truncate at LSN 0 when WAL has multiple segments
        let (_dir1, mut wal1) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _ in 0..5 { wal1.append_bytes(entry_data).await.unwrap(); } // Added .await // LSNs 0,1,2,3,4. Closed: (0,1), (2,3). Open: (4) start_lsn=4
        assert_eq!(wal1.closed_segments.len(), 2);
        assert_eq!(wal1.open_segment.segment.len(), 1);
        
        wal1.truncate_log_from_lsn(0).unwrap();
        assert!(wal1.closed_segments.is_empty(), "All closed segments should be gone");
        assert!(wal1.open_segment.segment.is_empty(), "Open segment should be empty");
        assert_eq!(wal1.open_segment.start_lsn, 4, "Open segment's designated start_lsn should remain");
        assert!(wal1.first_lsn().is_none(), "WAL should appear empty (first_lsn)");
        assert!(wal1.last_lsn().is_none(), "WAL should appear empty (last_lsn)");

        // Scenario 2: Truncate at last_lsn()
        let (_dir2, mut wal2) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _ in 0..5 { wal2.append_bytes(entry_data).await.unwrap(); } // Added .await // LSNs 0,1,2,3,4. Last LSN is 4.
        let last_lsn_before_truncate = wal2.last_lsn().unwrap();
        assert_eq!(last_lsn_before_truncate, 4);
        
        wal2.truncate_log_from_lsn(last_lsn_before_truncate).unwrap(); // Truncate LSN 4
        assert_eq!(wal2.last_lsn(), Some(3), "Last LSN should be 3");
        assert_eq!(wal2.closed_segments.len(), 2); // (0,1), (2,3)
        assert_eq!(wal2.open_segment.segment.len(), 0, "Open segment should be empty");
        assert_eq!(wal2.open_segment.start_lsn, 4);

        // Scenario 3: Truncate in the middle of a closed segment (not the last closed one)
        let (_dir3, mut wal3) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _ in 0..7 { wal3.append_bytes(entry_data).await.unwrap(); } // Added .await // LSNs 0..6. Closed: (0,1), (2,3), (4,5). Open: (6) start_lsn=6
        assert_eq!(wal3.closed_segments.len(), 3);
        assert_eq!(wal3.closed_segments[0].segment.len(), 2); // 0,1
        assert_eq!(wal3.closed_segments[1].segment.len(), 2); // 2,3
        assert_eq!(wal3.closed_segments[2].segment.len(), 2); // 4,5
        
        wal3.truncate_log_from_lsn(3).unwrap(); // Truncate from LSN 3 (in middle of closed segment 2,3)
        // Expected: closed[0]=(0,1), closed[1]=(2) (len 1, start_index 2). Others (closed[2], open) deleted/emptied.
        assert_eq!(wal3.closed_segments.len(), 2, "Should have 2 closed segments left");
        assert_eq!(wal3.closed_segments[0].start_index, 0);
        assert_eq!(wal3.closed_segments[0].segment.len(), 2); // (0,1)
        assert_eq!(wal3.closed_segments[1].start_index, 2);
        assert_eq!(wal3.closed_segments[1].segment.len(), 1); // (2)
        assert!(wal3.open_segment.segment.is_empty(), "Open segment should be empty");
        assert_eq!(wal3.open_segment.start_lsn, 6); // Its designated start LSN
        assert_eq!(wal3.last_lsn(), Some(2));

        // Scenario 4: Truncate an already empty WAL
        let (_dir4, mut wal4) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        assert!(wal4.first_lsn().is_none());
        wal4.truncate_log_from_lsn(0).unwrap();
        assert!(wal4.first_lsn().is_none());
        wal4.truncate_log_from_lsn(100).unwrap();
        assert!(wal4.first_lsn().is_none());

        // Scenario 5: Truncate to an LSN beyond current last_lsn()
        let (_dir5, mut wal5) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        wal5.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 0
        wal5.append_bytes(entry_data).await.unwrap(); // Added .await // LSN 1
        assert_eq!(wal5.last_lsn(), Some(1));
        wal5.truncate_log_from_lsn(100).unwrap(); // from_lsn (100) > open_segment.start_lsn (0)
                                                 // from_lsn (100) - open_segment.start_lsn (0) = 100.
                                                 // open_segment.len() is 2.
                                                 // 100 < 2 is false. So open_segment is not truncated.
        assert_eq!(wal5.last_lsn(), Some(1), "Truncating beyond last LSN should be no-op on existing entries");
        assert_eq!(wal5.open_segment.segment.len(), 2);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_prefix_truncate_complex_scenarios() { // Made async
        let entry_data = b"0123456789"; // Approx 28 bytes on disk
        let segment_capacity = 64; // Fits 2 entries

        // Scenario 1: Prefix truncate an empty WAL
        let (_dir1, mut wal1) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        wal1.prefix_truncate_log_until_lsn(10).unwrap();
        assert!(wal1.first_lsn().is_none());
        assert!(wal1.closed_segments.is_empty());
        assert!(wal1.open_segment.segment.is_empty());

        // Scenario 2: until_lsn is 0 or first_lsn (no-op)
        let (_dir2, mut wal2) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _ in 0..5 { wal2.append_bytes(entry_data).await.unwrap(); } // Added .await // LSNs 0..4. Closed: (0,1), (2,3). Open: (4)
        assert_eq!(wal2.first_lsn(), Some(0));
        wal2.prefix_truncate_log_until_lsn(0).unwrap(); // until_lsn <= first_lsn
        assert_eq!(wal2.closed_segments.len(), 2);
        assert_eq!(wal2.first_lsn(), Some(0));
        
        wal2.prefix_truncate_log_until_lsn(wal2.first_lsn().unwrap()).unwrap(); // until_lsn == first_lsn
        assert_eq!(wal2.closed_segments.len(), 2);
        assert_eq!(wal2.first_lsn(), Some(0));


        // Scenario 3: until_lsn is exactly last_lsn of a closed segment + 1 (deletes that segment)
        // WAL state: Closed: (0,1), (2,3). Open: (4) start_lsn=4. first_lsn=0, last_lsn=4
        // Last LSN of closed[0] is 1. until_lsn = 2.
        // Segment (0,1) ends at LSN 1. 1 < 2. So (0,1) should be deleted.
        wal2.prefix_truncate_log_until_lsn(2).unwrap(); 
        assert_eq!(wal2.closed_segments.len(), 1, "Segment (0,1) should be deleted");
        assert_eq!(wal2.closed_segments[0].start_index, 2, "Remaining closed segment should be (2,3)");
        assert_eq!(wal2.first_lsn(), Some(2));
        assert_eq!(wal2.last_lsn(), Some(4));

        // Scenario 4: until_lsn is in the middle of a closed segment (deletes segments before it)
        // WAL state: Closed: (2,3). Open: (4) start_lsn=4. first_lsn=2, last_lsn=4
        // until_lsn = 3. Segment (2,3) ends at LSN 3. 3 is not < 3. So (2,3) is NOT deleted.
        // The logic is: delete if segment_end_lsn < until_lsn.
        // Or, keep if segment_end_lsn >= until_lsn.
        // Segment (2,3) ends at 3. until_lsn = 3. 3 >= 3. So (2,3) is kept.
        wal2.prefix_truncate_log_until_lsn(3).unwrap();
        assert_eq!(wal2.closed_segments.len(), 1, "Segment (2,3) should be kept");
        assert_eq!(wal2.closed_segments[0].start_index, 2);
        assert_eq!(wal2.first_lsn(), Some(2));

        // Scenario 5: until_lsn makes all closed segments deleted
        // WAL state: Closed: (2,3). Open: (4) start_lsn=4.
        // until_lsn = 4. Segment (2,3) ends at LSN 3. 3 < 4. So (2,3) is deleted.
        wal2.prefix_truncate_log_until_lsn(4).unwrap();
        assert!(wal2.closed_segments.is_empty(), "All closed segments should be deleted");
        assert_eq!(wal2.first_lsn(), Some(4), "First LSN should be start of open segment");
        assert_eq!(wal2.open_segment.segment.len(), 1); // Entry for LSN 4

        // Scenario 6: until_lsn is >= open_segment.start_lsn (deletes all closed, open segment untouched)
        let (_dir3, mut wal3) = create_test_wal_with_options(default_test_options(segment_capacity)).await; // Added .await
        for _ in 0..5 { wal3.append_bytes(entry_data).await.unwrap(); } // Added .await // LSNs 0..4. Closed: (0,1), (2,3). Open: (4) start_lsn=4
        wal3.prefix_truncate_log_until_lsn(4).unwrap(); // until_lsn is open_segment.start_lsn
        assert!(wal3.closed_segments.is_empty());
        assert_eq!(wal3.open_segment.segment.len(), 1); // LSN 4
        assert_eq!(wal3.open_segment.start_lsn, 4);
        assert_eq!(wal3.first_lsn(), Some(4));

        wal3.prefix_truncate_log_until_lsn(5).unwrap(); // until_lsn is open_segment.last_lsn + 1
        assert!(wal3.closed_segments.is_empty());
        assert_eq!(wal3.open_segment.segment.len(), 1);
        assert_eq!(wal3.first_lsn(), Some(4));

        wal3.prefix_truncate_log_until_lsn(100).unwrap(); // until_lsn is far beyond
        assert!(wal3.closed_segments.is_empty());
        assert_eq!(wal3.open_segment.segment.len(), 1);
        assert_eq!(wal3.first_lsn(), Some(4));
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_complex_interactions_and_reopen() { // Made async
        let entry_data = b"interaction_data"; // 16 bytes. On disk: 8+16+0+4 = 28 bytes.
        let segment_capacity = 64; // Fits 2 entries.
        let options = default_test_options(segment_capacity);
        let dir = tempdir().expect("Failed to create temp dir for complex interaction test");
        let wal_path = dir.path().to_path_buf();

        {
            let mut wal = VortexWal::open(&wal_path, options.clone()).unwrap();

            // 1. Initial appends (LSN 0-4), creating closed segments
            // Entry "interaction_data_X" is ~18 bytes. On disk ~36 bytes. Segment capacity 64. -> 1 entry per segment.
            for i in 0..5 { // LSN 0,1,2,3,4
                let data_with_id = format!("{}_{}", String::from_utf8_lossy(entry_data), i);
                wal.append_bytes(data_with_id.as_bytes()).await.unwrap(); // Added .await
            }
            // State: Closed: (0), (1), (2), (3). Open: (4) start_lsn=4. first=0, last=4
            assert_eq!(wal.closed_segments.len(), 4, "Initial closed segments count");
            assert_eq!(wal.open_segment.segment.len(), 1, "Initial open segment length");
            assert_eq!(wal.last_lsn(), Some(4), "Initial last LSN");

            // 2. Truncate from LSN 3
            wal.truncate_log_from_lsn(3).unwrap();
            // State: Closed: (0), (1), (2). Open: () start_lsn=4. first=0, last=2
            assert_eq!(wal.closed_segments.len(), 3, "Closed segments after truncate(3)");
            assert_eq!(wal.closed_segments[2].start_index, 2, "Last closed segment start_index after truncate(3)");
            assert_eq!(wal.closed_segments[2].segment.len(), 1, "Last closed segment length after truncate(3)"); // Entry 2
            assert!(wal.open_segment.segment.is_empty(), "Open segment empty after truncate(3)");
            assert_eq!(wal.last_lsn(), Some(2), "Last LSN after truncate(3)");
            
            // 3. Append more entries ("reappend_3", "reappend_4", "reappend_5")
            // These will be LSNs 4, 5, 6 due to open_segment.start_lsn=4 and subsequent retirements.
            // Entry "interaction_data_reappend_X" is ~26 bytes. On disk ~44 bytes. Still 1 entry per segment.
            for i in 3..6 { 
                let data_with_id = format!("{}_reappend_{}", String::from_utf8_lossy(entry_data), i);
                wal.append_bytes(data_with_id.as_bytes()).await.unwrap(); // Added .await
            }
            // After LSN for "reappend_3" (becomes LSN 4): Closed: (0),(1),(2). Open: (4, LSN 4).
            // After LSN for "reappend_4" (becomes LSN 5): Retire(4). Closed: (0),(1),(2),(4). Open: (5, LSN 5).
            // After LSN for "reappend_5" (becomes LSN 6): Retire(5). Closed: (0),(1),(2),(4),(5). Open: (6, LSN 6).
            assert_eq!(wal.last_lsn(), Some(6), "Last LSN after re-appends");
            assert_eq!(wal.closed_segments.len(), 5, "Closed segments after re-appends. Segments: (0),(1),(2),(4),(5)");
            assert_eq!(wal.open_segment.segment.len(), 1, "Open segment after re-appends (entry for LSN 6)");
            assert_eq!(wal.open_segment.start_lsn, 6, "Open segment start_lsn after re-appends");

            // 4. Prefix truncate until LSN 5
            // Closed: (0),(1),(2),(4),(5). Open: (6).
            // until_lsn = 5.
            // Seg (0) ends 0. 0 < 5. Delete.
            // Seg (1) ends 1. 1 < 5. Delete.
            // Seg (2) ends 2. 2 < 5. Delete.
            // Seg (4) ends 4. 4 < 5. Delete.
            // Seg (5) ends 5. 5 is not < 5 (segment end LSN is start_index + len -1).
            //   Logic: keep if (cs.start_index + cs.segment.len() as u64 > until_lsn)
            //   For seg (5): 5 + 1 > 5 is true. Keep.
            // State: Closed: (5). Open: (6). first=5, last=6
            wal.prefix_truncate_log_until_lsn(5).unwrap();
            assert_eq!(wal.closed_segments.len(), 1, "Closed segments after prefix_truncate(5)");
            assert_eq!(wal.closed_segments[0].start_index, 5, "Remaining closed segment start_index");
            assert_eq!(wal.closed_segments[0].segment.len(), 1, "Remaining closed segment length"); // Entry for LSN 5 ("reappend_5")
            assert_eq!(wal.open_segment.start_lsn, 6, "Open segment start_lsn after prefix_truncate");
            assert_eq!(wal.first_lsn(), Some(5), "First LSN after prefix_truncate(5)");
            assert_eq!(wal.last_lsn(), Some(6), "Last LSN after prefix_truncate(5)");

            // 5. Append more entries ("final_7", "final_8")
            // Entry "interaction_data_final_X" is ~23 bytes. On disk ~40 bytes. Still 1 entry per segment.
            for i in 7..9 {
                let data_with_id = format!("{}_final_{}", String::from_utf8_lossy(entry_data), i);
                wal.append_bytes(data_with_id.as_bytes()).await.unwrap(); // Added .await
            }
            // Current: Closed: (5). Open: (6, LSN 6).
            // Append "final_7" (becomes LSN 7): Retire(6). Closed: (5),(6). Open: (7, LSN 7).
            // Append "final_8" (becomes LSN 8): Retire(7). Closed: (5),(6),(7). Open: (8, LSN 8).
            assert_eq!(wal.last_lsn(), Some(8), "Last LSN after final appends");
            assert_eq!(wal.closed_segments.len(), 3, "Closed segments after final appends. Segments: (5),(6),(7)");
            assert_eq!(wal.open_segment.segment.len(), 1, "Open segment after final appends (entry for LSN 8)");
            assert_eq!(wal.open_segment.start_lsn, 8, "Open segment start_lsn after final appends");
            
            // Verify some data points before close
            assert_eq!(&*wal.read_bytes_by_lsn(5).unwrap(), b"interaction_data_reappend_4"); // This was LSN 5
            assert_eq!(&*wal.read_bytes_by_lsn(8).unwrap(), b"interaction_data_final_8");
        } // WAL is dropped here, flushing and closing files.

        // 7. Reopen WAL
        let mut reopened_wal = VortexWal::open(&wal_path, options.clone()).unwrap();
        assert_eq!(reopened_wal.first_lsn(), Some(5), "Reopened WAL first LSN mismatch"); // Was 5
        assert_eq!(reopened_wal.last_lsn(), Some(8), "Reopened WAL last LSN mismatch");
        assert_eq!(reopened_wal.closed_segments.len(), 3, "Reopened WAL closed segments count mismatch"); // (5),(6),(7)
        assert_eq!(reopened_wal.closed_segments[0].start_index, 5);
        assert_eq!(reopened_wal.closed_segments[0].segment.len(), 1); 
        assert_eq!(reopened_wal.closed_segments[1].start_index, 6);
        assert_eq!(reopened_wal.closed_segments[1].segment.len(), 1); 
        assert_eq!(reopened_wal.closed_segments[2].start_index, 7);
        assert_eq!(reopened_wal.closed_segments[2].segment.len(), 1); 
        
        assert_eq!(reopened_wal.open_segment.segment.len(), 1, "Reopened WAL open segment len mismatch"); // Entry 8
        assert_eq!(reopened_wal.open_segment.start_lsn, 8, "Reopened WAL open segment start_lsn mismatch");

        // Verify data points after reopen
        assert_eq!(&*reopened_wal.read_bytes_by_lsn(5).unwrap(), b"interaction_data_reappend_4");
        assert_eq!(&*reopened_wal.read_bytes_by_lsn(8).unwrap(), b"interaction_data_final_8");
        assert!(reopened_wal.read_bytes_by_lsn(3).is_none(), "LSN 3 should not exist after reopen"); // LSN 3 was "interaction_data_3", then truncated.

        // 8. Append a final entry to the reopened WAL
        let final_entry_data = b"final_append_after_reopen";
        let lsn9 = reopened_wal.append_bytes(final_entry_data).await.unwrap(); // Added .await
        assert_eq!(lsn9, 9, "LSN of final append mismatch");
        assert_eq!(reopened_wal.last_lsn(), Some(9));
        // Open segment was (8, LSN 8). Appending "final_append_after_reopen" (LSN 9).
        // This should retire segment (8).
        // Closed: (5),(6),(7),(8). Open: (9, LSN 9).
        assert_eq!(reopened_wal.closed_segments.len(), 4); 
        assert_eq!(reopened_wal.open_segment.segment.len(), 1); 
        assert_eq!(reopened_wal.open_segment.start_lsn, 9);
        assert_eq!(&*reopened_wal.read_bytes_by_lsn(9).unwrap(), final_entry_data);
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_open_corrupted_segment_files() { // Made async
        let entry_data = b"good_data";
        let segment_capacity = 128; // Enough for a few entries
        let options = default_test_options(segment_capacity);
        let dir = tempdir().expect("Failed to create temp dir for corrupted segment test");
        let wal_path = dir.path();

        // Scenario 1: Closed segment with CRC mismatch
        let closed_segment_path = wal_path.join("closed-0");
        {
            let mut seg = VortexSegment::create(&closed_segment_path, segment_capacity).unwrap();
            seg.append_record_bytes(entry_data).unwrap(); // LSN 0
            seg.append_record_bytes(entry_data).unwrap(); // LSN 1
            seg.flush().unwrap(); // Flushes good data including CRCs

            // Manually corrupt the CRC of the second entry
            // Second entry data starts after first entry.
            // First entry: header(8) + data(9) + pad(7) + crc(4) = 28 bytes. Data offset 16.
            // Second entry header starts at offset 8 (seg_header) + 28 = 36.
            // Second entry data starts at 36 + 8 = 44.
            // Second entry data (9) + pad(3) = 12 bytes for data+padding. CRC is after this.
            // CRC offset for second entry: 40 (data_start) + 9 (data_len) + 3 (pad_len) = 52
            let mut file = OpenOptions::new().write(true).open(&closed_segment_path).unwrap();
            use std::io::{Seek, SeekFrom, Write};
            file.seek(SeekFrom::Start(52)).unwrap(); // Seek to CRC of second entry (offset 52)
            file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap(); // Write bad CRC
            file.flush().unwrap();
        }

        // Create a valid open segment file after the corrupted one to ensure WAL tries to open past it
        let open_segment_path = wal_path.join("open-0"); // ID 0, will be processed by creator
        {
            let mut open_seg_data = VortexSegment::create(&open_segment_path, segment_capacity).unwrap();
            // This segment will be empty, its start_lsn will be determined by VortexWal::open
            // based on the valid part of closed-0.
            open_seg_data.flush().unwrap();
        }


        let wal = VortexWal::open(wal_path, options.clone()).unwrap();
        // VortexSegment::open for "closed-0" should detect CRC mismatch for entry 1,
        // effectively truncating it to just entry 0.
        assert_eq!(wal.closed_segments.len(), 1, "Should have one (partially valid) closed segment");
        assert_eq!(wal.closed_segments[0].start_index, 0);
        assert_eq!(wal.closed_segments[0].segment.len(), 1, "Corrupted closed segment should only have 1 valid entry");
        assert_eq!(&*wal.read_bytes_by_lsn(0).unwrap(), entry_data);
        assert!(wal.read_bytes_by_lsn(1).is_none(), "Entry 1 should be gone due to CRC error");
        
        // The open segment should start after the valid part of the closed segment
        assert_eq!(wal.open_segment.start_lsn, 1, "Open segment should start at LSN 1");
        assert!(wal.open_segment.segment.is_empty());


        // Scenario 2: Segment file with invalid magic bytes
        let dir2 = tempdir().expect("Failed to create temp dir for invalid magic test");
        let wal_path2 = dir2.path();
        let bad_magic_segment_path = wal_path2.join("closed-0");
        {
            let mut file = OpenOptions::new().write(true).create(true).open(&bad_magic_segment_path).unwrap();
            file.write_all(b"BADMAGIC1234567890").unwrap(); // Invalid header
            file.set_len(64).unwrap(); // Ensure it has some size
            file.flush().unwrap();
        }

        match VortexWal::open(wal_path2, options.clone()) {
            Err(e) => {
                // This error comes from VortexSegment::open when it tries to read the bad segment
                assert_eq!(e.kind(), ErrorKind::InvalidData, "Expected InvalidData error for bad magic");
                assert!(e.to_string().contains("Illegal segment magic bytes") || e.to_string().contains("InvalidInput"));
            }
            Ok(_) => panic!("WAL open should have failed due to bad magic bytes in segment file"),
        }
    }
}
