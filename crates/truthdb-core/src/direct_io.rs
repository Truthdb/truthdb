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
        ring: IoUring,
        buffers: Vec<AlignedBuffer>,
        next_user_data: u64,
        len: u64,
    }

    impl DirectFile {
        pub fn open_existing(path: PathBuf) -> io::Result<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_DIRECT)
                .open(&path)?;
            Self::from_file(file)
        }

        pub fn create_new(path: PathBuf, total_size: u64) -> io::Result<Self> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o644)
                .custom_flags(libc::O_DIRECT)
                .open(&path)?;
            file.set_len(total_size)?;
            Self::from_file(file)
        }

        fn from_file(file: File) -> io::Result<Self> {
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
                ring,
                buffers,
                next_user_data: 1,
                len,
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
            {
                let mut submission = self.ring.submission();
                unsafe {
                    submission
                        .push(&entry)
                        .map_err(|_| io::Error::other("io_uring submission queue full"))?;
                }
            }

            self.ring
                .submit_and_wait(1)
                .map_err(annotate_io_uring_error)?;
            let mut completion = self.ring.completion();
            let cqe = completion
                .next()
                .ok_or_else(|| io::Error::other("io_uring completion queue empty"))?;
            if cqe.result() < 0 {
                return Err(io::Error::from_raw_os_error(-cqe.result()));
            }
            Ok(cqe.result() as usize)
        }

        fn next_user_data(&mut self) -> u64 {
            let user_data = self.next_user_data;
            self.next_user_data = self.next_user_data.saturating_add(1);
            user_data
        }
    }

    unsafe impl Send for DirectFile {}

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
    }
}

pub use imp::DirectFile;
