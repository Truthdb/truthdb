use crate::storage_layout::PAGE_SIZE;

/// A page-sized, page-aligned, heap-allocated buffer suitable for O_DIRECT I/O.
///
/// Callers own these frames (buffer pool frames, WAL tail-page images) and pass
/// them to [`DirectFile::read_page_into`] / [`DirectFile::write_page_from`].
pub struct AlignedPageBuf {
    ptr: std::ptr::NonNull<u8>,
}

impl AlignedPageBuf {
    pub fn new() -> Self {
        let layout = std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE)
            .expect("page layout is valid");
        // SAFETY: layout has non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        let Some(ptr) = std::ptr::NonNull::new(ptr) else {
            std::alloc::handle_alloc_error(layout);
        };
        AlignedPageBuf { ptr }
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid for PAGE_SIZE bytes for the lifetime of self.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for PAGE_SIZE bytes and uniquely borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    pub fn zero(&mut self) {
        self.as_mut_slice().fill(0);
    }
}

impl Default for AlignedPageBuf {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for AlignedPageBuf {
    fn clone(&self) -> Self {
        let mut copy = AlignedPageBuf::new();
        copy.as_mut_slice().copy_from_slice(self.as_slice());
        copy
    }
}

impl Drop for AlignedPageBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(PAGE_SIZE, PAGE_SIZE)
            .expect("page layout is valid");
        // SAFETY: ptr was allocated with this exact layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) };
    }
}

// SAFETY: AlignedPageBuf uniquely owns its allocation.
unsafe impl Send for AlignedPageBuf {}

#[cfg(target_os = "linux")]
mod imp {
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::PathBuf;
    use std::ptr;
    use std::slice;

    use io_uring::{IoUring, opcode, types};
    use libc::{c_void, iovec};

    use crate::storage_layout::{PAGE_SIZE, align_down};

    const IO_URING_ENTRIES: u32 = 8;
    const FIXED_FILE_INDEX: u32 = 0;
    const FIXED_BUFFER_INDEX: u16 = 0;

    struct AlignedBuffer {
        ptr: *mut u8,
        len: usize,
    }

    impl AlignedBuffer {
        fn new(len: usize, alignment: usize) -> io::Result<Self> {
            let mut ptr = ptr::null_mut();
            let rc = unsafe { libc::posix_memalign(&mut ptr, alignment, len) };
            if rc != 0 {
                return Err(io::Error::from_raw_os_error(rc));
            }
            unsafe {
                ptr::write_bytes(ptr.cast::<u8>(), 0, len);
            }
            Ok(Self {
                ptr: ptr.cast::<u8>(),
                len,
            })
        }

        fn as_iovec(&self) -> iovec {
            iovec {
                iov_base: self.ptr.cast::<c_void>(),
                iov_len: self.len,
            }
        }

        fn as_slice(&self) -> &[u8] {
            unsafe { slice::from_raw_parts(self.ptr, self.len) }
        }

        fn as_mut_slice(&mut self) -> &mut [u8] {
            unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
        }
    }

    impl Drop for AlignedBuffer {
        fn drop(&mut self) {
            if !self.ptr.is_null() {
                unsafe {
                    libc::free(self.ptr.cast::<c_void>());
                }
            }
        }
    }

    unsafe impl Send for AlignedBuffer {}

    pub struct DirectFile {
        file: File,
        /// Holds the advisory lock (see [`Self::lock_exclusive`]). A separate
        /// plain fd rather than a lock on `file`: the O_DIRECT fd is
        /// registered with io_uring, whose kernel-side file release at ring
        /// teardown is deferred, so a lock on it would linger past drop. This
        /// fd closes synchronously, releasing the lock at drop.
        _lock: Option<File>,
        ring: IoUring,
        buffers: Vec<AlignedBuffer>,
        next_user_data: u64,
        len: u64,
        /// Set when the ring state is no longer trustworthy (submission or
        /// wait failed with operations possibly in flight). All further
        /// operations fail fast: consuming stale completions would let a
        /// later fsync acknowledge a write that never happened.
        poisoned: bool,
    }

    impl DirectFile {
        pub fn open_existing(path: PathBuf) -> io::Result<Self> {
            let lock = Self::lock_exclusive(&path)?;
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)?;
            Self::from_file(file, Some(lock))
        }

        /// The storage layer's SECOND handle on a file its primary handle
        /// already holds the advisory lock for (the log writer's). Taking
        /// the lock here would conflict with our own primary.
        pub fn open_existing_unlocked(path: PathBuf) -> io::Result<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)?;
            Self::from_file(file, None)
        }

        pub fn create_new(path: PathBuf, total_size: u64) -> io::Result<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o644)
                .custom_flags(libc::O_DIRECT)
                .open(&path)?;
            let lock = Self::lock_exclusive(&path)?;
            file.set_len(total_size)?;
            Self::from_file(file, Some(lock))
        }

        /// Advisory whole-file lock, held for the life of the handle: the
        /// server keeps it while running, so the offline `grow` command's
        /// open fails fast instead of operating under a live server (and a
        /// second server open fails instead of corrupting). Advisory only —
        /// it fences TruthDB processes, not arbitrary writers. flock is per
        /// open file description, so two opens in one process conflict too.
        fn lock_exclusive(path: &PathBuf) -> io::Result<File> {
            let lock = OpenOptions::new().read(true).open(path)?;
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(lock);
            }
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!(
                        "{} is locked by another TruthDB process; stop it first",
                        path.display()
                    ),
                ));
            }
            Err(err)
        }

        fn from_file(file: File, lock: Option<File>) -> io::Result<Self> {
            let len = file.metadata()?.len();
            let buffers = vec![AlignedBuffer::new(PAGE_SIZE, PAGE_SIZE)?];
            let ring = IoUring::new(IO_URING_ENTRIES).map_err(annotate_io_uring_error)?;

            let iovecs = buffers
                .iter()
                .map(AlignedBuffer::as_iovec)
                .collect::<Vec<_>>();
            unsafe {
                ring.submitter()
                    .register_buffers(&iovecs)
                    .map_err(annotate_io_uring_error)?;
            }
            ring.submitter()
                .register_files(&[file.as_raw_fd()])
                .map_err(annotate_io_uring_error)?;

            Ok(Self {
                file,
                _lock: lock,
                ring,
                buffers,
                next_user_data: 1,
                len,
                poisoned: false,
            })
        }

        pub fn len(&self) -> u64 {
            self.len
        }

        pub fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
            if buf.is_empty() {
                return Ok(());
            }

            let page_size_u64 = PAGE_SIZE as u64;
            let mut filled = 0usize;
            let mut current_offset = offset;

            while filled < buf.len() {
                let page_offset = align_down(current_offset, page_size_u64);
                let within_page = (current_offset - page_offset) as usize;
                let copy_len = (PAGE_SIZE - within_page).min(buf.len() - filled);
                self.read_page(page_offset)?;

                let scratch = self
                    .buffers
                    .get(FIXED_BUFFER_INDEX as usize)
                    .expect("direct I/O scratch buffer missing");
                buf[filled..filled + copy_len]
                    .copy_from_slice(&scratch.as_slice()[within_page..within_page + copy_len]);
                current_offset = current_offset.saturating_add(copy_len as u64);
                filled += copy_len;
            }

            Ok(())
        }

        pub fn write_all_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
            if data.is_empty() {
                return Ok(());
            }

            let page_size_u64 = PAGE_SIZE as u64;
            let mut written = 0usize;
            let mut current_offset = offset;

            while written < data.len() {
                let page_offset = align_down(current_offset, page_size_u64);
                let within_page = (current_offset - page_offset) as usize;
                let copy_len = (PAGE_SIZE - within_page).min(data.len() - written);
                let whole_page_write = within_page == 0 && copy_len == PAGE_SIZE;

                if whole_page_write {
                    let scratch = self
                        .buffers
                        .get_mut(FIXED_BUFFER_INDEX as usize)
                        .expect("direct I/O scratch buffer missing");
                    scratch
                        .as_mut_slice()
                        .copy_from_slice(&data[written..written + copy_len]);
                } else {
                    self.read_page(page_offset)?;
                    let scratch = self
                        .buffers
                        .get_mut(FIXED_BUFFER_INDEX as usize)
                        .expect("direct I/O scratch buffer missing");
                    scratch.as_mut_slice()[within_page..within_page + copy_len]
                        .copy_from_slice(&data[written..written + copy_len]);
                }

                self.write_page(page_offset)?;
                current_offset = current_offset.saturating_add(copy_len as u64);
                written += copy_len;
            }

            Ok(())
        }

        pub fn sync_data(&mut self) -> io::Result<()> {
            let entry = opcode::Fsync::new(types::Fixed(FIXED_FILE_INDEX))
                .build()
                .user_data(self.next_user_data());
            let _ = self.submit(entry)?;
            let _ = self.file.as_raw_fd();
            Ok(())
        }

        /// Duplicates the underlying file descriptor as a plain [`std::fs::File`]
        /// (same open file description). The clone shares the file, so an
        /// `fdatasync` on it flushes every write made through this handle — used
        /// by the group-commit log-writer to fsync the WAL off the storage lock,
        /// without touching this handle's io_uring ring.
        pub fn try_clone_std(&self) -> io::Result<std::fs::File> {
            self.file.try_clone()
        }

        pub fn read_page_into(
            &mut self,
            page_offset: u64,
            frame: &mut super::AlignedPageBuf,
        ) -> io::Result<()> {
            ensure_page_aligned(page_offset)?;
            let entry = opcode::Read::new(
                types::Fixed(FIXED_FILE_INDEX),
                frame.as_mut_slice().as_mut_ptr(),
                PAGE_SIZE as u32,
            )
            .offset(page_offset)
            .build()
            .user_data(self.next_user_data());

            let read = self.submit(entry)?;
            if read != PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short direct read: expected {PAGE_SIZE}, got {read}"),
                ));
            }
            Ok(())
        }

        pub fn write_page_from(
            &mut self,
            page_offset: u64,
            frame: &super::AlignedPageBuf,
        ) -> io::Result<()> {
            self.write_pages_from(page_offset, &[frame])
        }

        /// Writes `frames` as consecutive pages starting at `page_offset`,
        /// batching submissions through the ring.
        pub fn write_pages_from(
            &mut self,
            page_offset: u64,
            frames: &[&super::AlignedPageBuf],
        ) -> io::Result<()> {
            self.check_usable()?;
            ensure_page_aligned(page_offset)?;
            for (chunk_index, chunk) in frames.chunks(IO_URING_ENTRIES as usize).enumerate() {
                let chunk_offset =
                    page_offset + (chunk_index * IO_URING_ENTRIES as usize * PAGE_SIZE) as u64;
                for (i, frame) in chunk.iter().enumerate() {
                    let entry = opcode::Write::new(
                        types::Fixed(FIXED_FILE_INDEX),
                        frame.as_slice().as_ptr(),
                        PAGE_SIZE as u32,
                    )
                    .offset(chunk_offset + (i * PAGE_SIZE) as u64)
                    .build()
                    .user_data(self.next_user_data());
                    // SAFETY: the frame borrows outlive this call, and every
                    // queued entry is either fully submitted-and-reaped below
                    // or the handle is poisoned so the ring is never touched
                    // again.
                    unsafe { self.push_entry(&entry)? };
                }
                self.submit_and_wait_all(chunk.len())?;
                self.drain_completions(chunk.len(), PAGE_SIZE)?;
            }
            Ok(())
        }

        fn read_page(&mut self, page_offset: u64) -> io::Result<()> {
            let user_data = self.next_user_data();
            let buf_ptr = {
                let scratch = self
                    .buffers
                    .get_mut(FIXED_BUFFER_INDEX as usize)
                    .expect("direct I/O scratch buffer missing");
                scratch.as_mut_slice().as_mut_ptr()
            };
            let entry = opcode::ReadFixed::new(
                types::Fixed(FIXED_FILE_INDEX),
                buf_ptr,
                PAGE_SIZE as u32,
                FIXED_BUFFER_INDEX,
            )
            .offset(page_offset)
            .build()
            .user_data(user_data);

            let read = self.submit(entry)?;
            if read != PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("short direct read: expected {PAGE_SIZE}, got {read}"),
                ));
            }
            Ok(())
        }

        fn write_page(&mut self, page_offset: u64) -> io::Result<()> {
            let user_data = self.next_user_data();
            let buf_ptr = {
                let scratch = self
                    .buffers
                    .get(FIXED_BUFFER_INDEX as usize)
                    .expect("direct I/O scratch buffer missing");
                scratch.as_slice().as_ptr()
            };
            let entry = opcode::WriteFixed::new(
                types::Fixed(FIXED_FILE_INDEX),
                buf_ptr,
                PAGE_SIZE as u32,
                FIXED_BUFFER_INDEX,
            )
            .offset(page_offset)
            .build()
            .user_data(user_data);

            let written = self.submit(entry)?;
            if written != PAGE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!("short direct write: expected {PAGE_SIZE}, got {written}"),
                ));
            }
            Ok(())
        }

        fn submit(&mut self, entry: io_uring::squeue::Entry) -> io::Result<usize> {
            self.check_usable()?;
            // SAFETY: the entry's buffer is either an internal scratch buffer
            // owned by self or a caller frame that outlives the call; the
            // entry is submitted and reaped (or the handle poisoned) below.
            unsafe { self.push_entry(&entry)? };
            self.submit_and_wait_all(1)?;

            let cqe = match self.ring.completion().next() {
                Some(cqe) => cqe,
                None => {
                    self.poisoned = true;
                    return Err(io::Error::other("io_uring completion queue empty"));
                }
            };
            if cqe.result() < 0 {
                return Err(io::Error::from_raw_os_error(-cqe.result()));
            }
            Ok(cqe.result() as usize)
        }

        fn check_usable(&self) -> io::Result<()> {
            if self.poisoned {
                return Err(io::Error::other(
                    "direct file disabled after an io_uring failure (operations may still be in flight); refusing to acknowledge further I/O",
                ));
            }
            Ok(())
        }

        /// Queues one SQE. On failure the handle is poisoned: earlier entries
        /// of the same batch may already sit unsubmitted in the queue, and
        /// they must never reach the kernel once their buffers are gone.
        ///
        /// # Safety
        /// The buffers referenced by `entry` must stay live until the
        /// matching completion is reaped, or the handle is poisoned.
        unsafe fn push_entry(&mut self, entry: &io_uring::squeue::Entry) -> io::Result<()> {
            let pushed = unsafe { self.ring.submission().push(entry) };
            if pushed.is_err() {
                self.poisoned = true;
                return Err(io::Error::other("io_uring submission queue full"));
            }
            Ok(())
        }

        /// Submits queued SQEs and waits for `want` completions, retrying
        /// interrupted waits. Any other failure poisons the handle:
        /// operations may be in flight against buffers we no longer control.
        fn submit_and_wait_all(&mut self, want: usize) -> io::Result<()> {
            loop {
                match self.ring.submit_and_wait(want) {
                    Ok(_) => return Ok(()),
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        self.poisoned = true;
                        return Err(annotate_io_uring_error(err));
                    }
                }
            }
        }

        /// Reaps exactly `count` completions, even after one reports an
        /// error — an unreaped CQE would be mis-attributed to a later
        /// operation (e.g. an fsync acknowledging a failed write). Returns
        /// the first error encountered.
        fn drain_completions(&mut self, count: usize, expected_len: usize) -> io::Result<()> {
            let mut first_error: Option<io::Error> = None;
            let mut missing = false;
            {
                let mut completion = self.ring.completion();
                for _ in 0..count {
                    let Some(cqe) = completion.next() else {
                        missing = true;
                        break;
                    };
                    if cqe.result() < 0 {
                        first_error
                            .get_or_insert_with(|| io::Error::from_raw_os_error(-cqe.result()));
                    } else if cqe.result() as usize != expected_len {
                        first_error.get_or_insert_with(|| {
                            io::Error::new(
                                io::ErrorKind::WriteZero,
                                format!(
                                    "short direct write: expected {expected_len}, got {}",
                                    cqe.result()
                                ),
                            )
                        });
                    }
                }
            }
            if missing {
                self.poisoned = true;
                return Err(first_error
                    .unwrap_or_else(|| io::Error::other("io_uring completion queue empty")));
            }
            match first_error {
                Some(err) => Err(err),
                None => Ok(()),
            }
        }

        fn next_user_data(&mut self) -> u64 {
            let user_data = self.next_user_data;
            self.next_user_data = self.next_user_data.saturating_add(1);
            user_data
        }
    }

    unsafe impl Send for DirectFile {}

    fn ensure_page_aligned(offset: u64) -> io::Result<()> {
        if !offset.is_multiple_of(PAGE_SIZE as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {offset} is not page-aligned"),
            ));
        }
        Ok(())
    }

    fn annotate_io_uring_error(err: io::Error) -> io::Error {
        match err.raw_os_error() {
            Some(libc::ENOSYS) => io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is unavailable in this Linux runtime (ENOSYS); TruthDB now requires native Linux io_uring support, and emulated linux/amd64 containers may not provide it",
            ),
            Some(libc::EPERM) => io::Error::new(
                io::ErrorKind::PermissionDenied,
                "io_uring is blocked in this Linux runtime (EPERM); check container seccomp or runtime restrictions",
            ),
            _ => err,
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;

    pub struct DirectFile;

    impl DirectFile {
        pub fn open_existing(path: std::path::PathBuf) -> io::Result<Self> {
            Err(io::Error::other(format!(
                "truthdb storage requires Linux io_uring; unsupported platform for {}",
                path.display()
            )))
        }

        pub fn open_existing_unlocked(path: std::path::PathBuf) -> io::Result<Self> {
            Err(io::Error::other(format!(
                "truthdb storage requires Linux io_uring; unsupported platform for {}",
                path.display()
            )))
        }

        pub fn create_new(path: std::path::PathBuf, _total_size: u64) -> io::Result<Self> {
            Err(io::Error::other(format!(
                "truthdb storage requires Linux io_uring; unsupported platform for {}",
                path.display()
            )))
        }

        pub fn len(&self) -> u64 {
            0
        }

        pub fn read_exact_at(&mut self, _offset: u64, _buf: &mut [u8]) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn write_all_at(&mut self, _offset: u64, _data: &[u8]) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn sync_data(&mut self) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn try_clone_std(&self) -> io::Result<std::fs::File> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn read_page_into(
            &mut self,
            _page_offset: u64,
            _frame: &mut super::AlignedPageBuf,
        ) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn write_page_from(
            &mut self,
            _page_offset: u64,
            _frame: &super::AlignedPageBuf,
        ) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }

        pub fn write_pages_from(
            &mut self,
            _page_offset: u64,
            _frames: &[&super::AlignedPageBuf],
        ) -> io::Result<()> {
            Err(io::Error::other("truthdb storage requires Linux io_uring"))
        }
    }
}

pub use imp::DirectFile;
