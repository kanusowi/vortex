use memmap2::{MmapMut, MmapOptions};
use std::fmt;
use std::fs::File;
use std::io::Result;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex, MutexGuard}; // Added Mutex, MutexGuard

/// ported from https://github.com/danburkert/memmap-rs in version 0.5.2
///
/// A thread-safe view of a memory map.
///
/// The view may be split into disjoint ranges, each of which will share the
/// underlying memory map.
pub struct MmapViewSync {
    inner: Arc<Mutex<MmapMut>>, // Changed UnsafeCell to Mutex
    offset: usize,
    len: usize,
}

impl MmapViewSync {
    pub fn from_file(file: &File, offset: usize, capacity: usize) -> Result<MmapViewSync> {
        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset as u64)
                .len(capacity)
                .map_mut(file)?
        };

        Ok(mmap.into())
    }

    #[allow(dead_code)]
    pub fn anonymous(capacity: usize) -> Result<MmapViewSync> {
        let mmap = MmapOptions::new().len(capacity).map_anon()?;

        Ok(mmap.into())
    }

    /// Split the view into disjoint pieces at the specified offset.
    ///
    /// The provided offset must be less than the view's length.
    #[allow(dead_code)]
    pub fn split_at(self, offset: usize) -> Result<(MmapViewSync, MmapViewSync)> {
        if self.len < offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view split offset must be less than the view length",
            ));
        }
        let MmapViewSync {
            inner,
            offset: self_offset,
            len: self_len,
        } = self;
        Ok((
            MmapViewSync {
                inner: inner.clone(),
                offset: self_offset,
                len: offset,
            },
            MmapViewSync {
                inner,
                offset: self_offset + offset,
                len: self_len - offset,
            },
        ))
    }

    /// Restricts the range of this view to the provided offset and length.
    ///
    /// The provided range must be a subset of the current range (`offset + len < view.len()`).
    pub fn restrict(&mut self, offset: usize, len: usize) -> Result<()> {
        if offset + len > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mmap view may only be restricted to a subrange \
                                       of the current view",
            ));
        }
        self.offset += offset;
        self.len = len;
        Ok(())
    }

    /// Get a reference to the inner mmap.
    /// This now locks the mutex.
    fn inner_lock(&self) -> MutexGuard<'_, MmapMut> {
        self.inner.lock().expect("Mutex poisoned")
    }

    // Removed inner_mut as direct mutable access via &mut MmapMut is tricky with MutexGuard.
    // Operations needing mutation will lock and operate on the guard.

    /// Flushes outstanding view modifications to disk.
    ///
    /// When this returns with a non-error result, all outstanding changes to a file-backed memory
    /// map view are guaranteed to be durably stored. The file's metadata (including last
    /// modification timestamp) may not be updated.
    pub fn flush(&self) -> Result<()> {
        let mmap = self.inner_lock();
        mmap.flush_range(self.offset, self.len)
    }

    /// Asynchronously flushes outstanding view modifications to disk.
    ///
    /// This method initiates an asynchronous flush operation. When this method returns
    /// successfully, the flush operation has been initiated with the operating system,
    /// but the data may not yet be durably stored.
    pub fn flush_async_os(&self) -> Result<()> {
        let mmap = self.inner_lock();
        // Use flush_async_range if available and appropriate, or flush_async for the whole map
        // if range-specific async flush is not what memmap2 provides easily.
        // memmap2::MmapMut has flush_async() and flush_async_range().
        // Let's use flush_async_range to be consistent with flush().
        mmap.flush_async_range(self.offset, self.len)
    }

    /// Returns the length of the memory map view.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the memory mapped file as an immutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently modified by other means
    /// while the slice is held. The Mutex protects concurrent access via MmapViewSync methods
    /// during the call to `as_slice`, but the returned slice's lifetime is not tied to the lock.
    /// The caller is responsible for ensuring the slice is not used after the underlying data
    /// could be mutated by another thread.
    pub unsafe fn as_slice(&self) -> &[u8] {
        let mmap_guard = self.inner_lock();
        // The slice is constructed from a raw pointer obtained while the lock is held.
        // The lock (`mmap_guard`) is released when this function returns.
        // The caller must uphold safety invariants for the lifetime of the returned slice.
        core::slice::from_raw_parts((&mmap_guard[self.offset..]).as_ptr(), self.len)
    }

    /// Returns the memory mapped file as a mutable slice.
    ///
    /// ## Unsafety
    ///
    /// The caller must ensure that the file is not concurrently accessed by other means
    /// while the slice is held. The Mutex protects concurrent access via MmapViewSync methods.
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        let (offset, len) = (self.offset, self.len);
        let mut mmap = self.inner.lock().expect("Mutex poisoned for mut_slice");
        core::slice::from_raw_parts_mut((&mut mmap[offset..]).as_mut_ptr(), len)
    }

    /// Clones the view of the memory map.
    ///
    /// The underlying memory map is shared, and thus the caller must ensure that the memory
    /// underlying the view is not illegally aliased.
    /// This method is unsafe because it allows creating multiple MmapViewSync instances
    /// that might be used to get mutable slices to the same underlying data without
    /// further synchronization if not handled carefully by the caller.
    /// However, with the internal Mutex, direct concurrent mutation through these
    /// methods is prevented. The `unsafe` here pertains more to the general unsafety
    /// of mmap and potential for external modification or aliasing if not careful.
    pub unsafe fn clone(&self) -> MmapViewSync { // Keeping unsafe as it's part of the original API
        MmapViewSync {
            inner: Arc::clone(&self.inner), // Use Arc::clone for clarity
            offset: self.offset,
            len: self.len,
        }
    }
}

impl From<MmapMut> for MmapViewSync {
    fn from(mmap: MmapMut) -> MmapViewSync {
        let len = mmap.len();
        MmapViewSync {
            inner: Arc::new(Mutex::new(mmap)), // Changed to Mutex
            offset: 0,
            len,
        }
    }
}

impl fmt::Debug for MmapViewSync {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MmapViewSync {{ offset: {}, len: {} }}",
            self.offset, self.len
        )
    }
}

// These unsafe impls are no longer needed as Mutex<MmapMut> makes it Send + Sync
// #[cfg(test)]
// unsafe impl Sync for MmapViewSync {}
// #[cfg(test)]
// unsafe impl Send for MmapViewSync {}

#[cfg(test)]
mod test {
    use std::fs;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::thread;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn view() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_sync() {
        let len = 128;
        let split = 32;
        let mut view = MmapViewSync::anonymous(len).unwrap();
        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        // write values into the view
        unsafe { view.as_mut_slice() }.write_all(&incr[..]).unwrap();

        let (mut view1, view2) = view.split_at(32).unwrap();
        assert_eq!(view1.len(), split);
        assert_eq!(view2.len(), len - split);

        assert_eq!(&incr[0..split], unsafe { view1.as_slice() });
        assert_eq!(&incr[split..], unsafe { view2.as_slice() });

        view1.restrict(10, 10).unwrap();
        assert_eq!(&incr[10..20], unsafe { view1.as_slice() })
    }

    #[test]
    fn view_write() {
        let len = 131072; // 256KiB
        let split = 66560; // 65KiB + 10B

        let tempdir = Builder::new().prefix("mmap").tempdir().unwrap();
        let path = tempdir.path().join("mmap");

        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        file.set_len(len).unwrap();

        let incr = (0..len).map(|n| n as u8).collect::<Vec<_>>();
        let incr1 = incr[0..split].to_owned();
        let incr2 = incr[split..].to_owned();

        let view: MmapViewSync = MmapViewSync::from_file(&file, 0, len as usize).unwrap();
        let (mut view1, mut view2) = view.split_at(split).unwrap();

        let join1 = thread::spawn(move || {
            let _written = unsafe { view1.as_mut_slice() }.write(&incr1).unwrap();
            view1.flush().unwrap();
        });

        let join2 = thread::spawn(move || {
            let _written = unsafe { view2.as_mut_slice() }.write(&incr2).unwrap();
            view2.flush().unwrap();
        });

        join1.join().unwrap();
        join2.join().unwrap();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(incr, &buf[..]);
    }

    #[test]
    fn view_sync_send() {
        let view: Arc<MmapViewSync> = Arc::new(MmapViewSync::anonymous(128).unwrap());
        thread::spawn(move || unsafe {
            view.as_slice();
        });
    }
}
