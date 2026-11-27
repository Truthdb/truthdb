//! A bucket (or a prefix of one) as a local file system, read-only, with
//! Mountpoint for Amazon S3's semantics: directories are what `/` divides
//! the keys into; a key ending in `/` is no file (a directory of that name
//! is); names `.`, `..`, empty ones and those holding NUL are not shown;
//! a file and a directory of one name: the directory is. Reads are ranged
//! GETs, read ahead. Files are `0644` and directories `0755`, the mounting
//! user's, unless set otherwise; inode numbers last as long as the mount.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    MountOption, OpenAccMode, OpenFlags, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request, SessionACL,
};
use truthdb_s3client::{Client, Error};

/// How long the kernel keeps what it was told of a file or a directory
/// (Mountpoint: "stale metadata for that object for up to 1 second").
const TTL: Duration = Duration::from_secs(1);
/// What one read fetches at least: the reads after it in the file are
/// answered from it.
const READ_AHEAD: u64 = 4 * 1024 * 1024;

/// What is mounted, and how it shows.
#[derive(Debug, Clone)]
pub struct Options {
    pub bucket: String,
    /// Empty, or ending in `/`: only the keys under it, without it.
    pub prefix: String,
    pub uid: u32,
    pub gid: u32,
    pub file_mode: u16,
    pub dir_mode: u16,
    /// Others than the mounting user may use the mount.
    pub allow_other: bool,
}

#[derive(Debug, Clone)]
enum Kind {
    Directory,
    File { size: u64, mtime: u64 },
}

#[derive(Debug, Clone)]
struct Node {
    /// Its path under the mount (empty: the root), with no trailing `/`.
    path: String,
    kind: Kind,
}

/// A file open for reading: what was last fetched of it.
struct Reader {
    key: String,
    size: u64,
    start: u64,
    data: Bytes,
}

#[derive(Default)]
struct State {
    nodes: HashMap<u64, Node>,
    by_path: HashMap<String, u64>,
    next_ino: u64,
    /// Directories open: their entries (inode, kind, name), as listed.
    listings: HashMap<u64, Vec<(u64, FileType, String)>>,
    readers: HashMap<u64, Reader>,
    next_fh: u64,
}

impl State {
    /// The inode of `path` as `kind`, given one if it has none.
    fn inode(&mut self, path: &str, kind: Kind) -> u64 {
        let ino = match self.by_path.get(path) {
            Some(ino) => *ino,
            None => {
                self.next_ino += 1;
                let ino = self.next_ino;
                self.by_path.insert(path.to_string(), ino);
                ino
            }
        };
        self.nodes.insert(
            ino,
            Node {
                path: path.to_string(),
                kind,
            },
        );
        ino
    }
}

/// The mounted bucket.
pub struct Mount {
    client: Client,
    options: Options,
    runtime: tokio::runtime::Runtime,
    state: Mutex<State>,
}

/// Whether `name` is shown as a file or a directory's name.
pub fn shown(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('\0') && !name.contains('/')
}

fn errno(err: &Error) -> Errno {
    match err {
        Error::Service { status: 404, .. } => Errno::ENOENT,
        Error::Service { status: 403, .. } => Errno::EACCES,
        _ => Errno::EIO,
    }
}

impl Mount {
    pub fn new(client: Client, options: Options) -> std::io::Result<Mount> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let mut state = State {
            next_ino: 1,
            ..State::default()
        };
        state.nodes.insert(
            INodeNo::ROOT.0,
            Node {
                path: String::new(),
                kind: Kind::Directory,
            },
        );
        state.by_path.insert(String::new(), INodeNo::ROOT.0);
        Ok(Mount {
            client,
            options,
            runtime,
            state: Mutex::new(state),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A path's key (a file's), or prefix (a directory's: ending in `/`).
    fn key(&self, path: &str) -> String {
        format!("{}{path}", self.options.prefix)
    }

    fn directory_prefix(&self, path: &str) -> String {
        match path {
            "" => self.options.prefix.clone(),
            path => format!("{}{path}/", self.options.prefix),
        }
    }

    fn attr(&self, ino: u64, kind: &Kind) -> FileAttr {
        let (kind, size, mtime, perm) = match kind {
            Kind::Directory => (FileType::Directory, 0, UNIX_EPOCH, self.options.dir_mode),
            Kind::File { size, mtime } => (
                FileType::RegularFile,
                *size,
                UNIX_EPOCH + Duration::from_secs(*mtime),
                self.options.file_mode,
            ),
        };
        FileAttr {
            ino: INodeNo(ino),
            size,
            blocks: size.div_ceil(512),
            // "Last access time and last status change time will be the
            // same as the last modified time."
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm,
            nlink: if kind == FileType::Directory { 2 } else { 1 },
            uid: self.options.uid,
            gid: self.options.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// What `path` is: a directory (a key under it), else a file (its key),
    /// else nothing.
    fn find(&self, path: &str) -> Result<Option<Kind>, Error> {
        let bucket = &self.options.bucket;
        self.runtime.block_on(async {
            let under = self
                .client
                .list_objects(bucket, &self.directory_prefix(path), Some("/"), None)
                .await?;
            if !under.objects.is_empty() || !under.prefixes.is_empty() {
                return Ok(Some(Kind::Directory));
            }
            // The key itself sorts first of those it begins.
            let key = self.key(path);
            let listed = self
                .client
                .list_objects(bucket, &key, Some("/"), None)
                .await?;
            Ok(listed
                .objects
                .into_iter()
                .find(|object| object.key == key)
                .map(|object| Kind::File {
                    size: object.size,
                    mtime: truthdb_s3client::iso_seconds(&object.last_modified).unwrap_or(0),
                }))
        })
    }

    /// A directory's entries, as shown.
    fn list(&self, path: &str) -> Result<Vec<(String, Kind)>, Error> {
        let bucket = &self.options.bucket;
        let prefix = self.directory_prefix(path);
        let (mut directories, mut files) = (Vec::new(), Vec::new());
        self.runtime.block_on(async {
            let mut next = None;
            loop {
                let page = self
                    .client
                    .list_objects(bucket, &prefix, Some("/"), next.as_deref())
                    .await?;
                for common in &page.prefixes {
                    let name = common[prefix.len()..].trim_end_matches('/');
                    if shown(name)
                        && common.ends_with('/')
                        && name.len() + 1 == common.len() - prefix.len()
                    {
                        directories.push(name.to_string());
                    }
                }
                for object in page.objects {
                    let name = &object.key[prefix.len()..];
                    if shown(name) {
                        files.push((
                            name.to_string(),
                            Kind::File {
                                size: object.size,
                                mtime: truthdb_s3client::iso_seconds(&object.last_modified)
                                    .unwrap_or(0),
                            },
                        ));
                    }
                }
                match page.next {
                    Some(token) => next = Some(token),
                    None => break,
                }
            }
            Ok::<_, Error>(())
        })?;
        let mut out: Vec<(String, Kind)> = directories
            .iter()
            .map(|name| (name.clone(), Kind::Directory))
            .collect();
        // A file of a directory's name is not shown.
        out.extend(
            files
                .into_iter()
                .filter(|(name, _)| !directories.contains(name)),
        );
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

fn child(parent: &str, name: &str) -> String {
    match parent {
        "" => name.to_string(),
        parent => format!("{parent}/{name}"),
    }
}

impl Filesystem for Mount {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str().filter(|name| shown(name)) else {
            return reply.error(Errno::ENOENT);
        };
        let Some(parent) = self
            .lock()
            .nodes
            .get(&parent.0)
            .map(|node| node.path.clone())
        else {
            return reply.error(Errno::ENOENT);
        };
        let path = child(&parent, name);
        match self.find(&path) {
            Ok(Some(kind)) => {
                let ino = self.lock().inode(&path, kind.clone());
                reply.entry(&TTL, &self.attr(ino, &kind), Generation(0));
            }
            Ok(None) => reply.error(Errno::ENOENT),
            Err(err) => reply.error(errno(&err)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let node = self.lock().nodes.get(&ino.0).cloned();
        match node {
            Some(node) => reply.attr(&TTL, &self.attr(ino.0, &node.kind)),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let Some(path) = self.lock().nodes.get(&ino.0).map(|node| node.path.clone()) else {
            return reply.error(Errno::ENOENT);
        };
        let listed = match self.list(&path) {
            Ok(listed) => listed,
            Err(err) => return reply.error(errno(&err)),
        };
        let mut state = self.lock();
        let parent_path = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        let parent = state
            .by_path
            .get(parent_path)
            .copied()
            .unwrap_or(INodeNo::ROOT.0);
        let mut entries = vec![
            (ino.0, FileType::Directory, ".".to_string()),
            (parent, FileType::Directory, "..".to_string()),
        ];
        for (name, kind) in listed {
            let file_type = match kind {
                Kind::Directory => FileType::Directory,
                Kind::File { .. } => FileType::RegularFile,
            };
            let child = state.inode(&child(&path, &name), kind);
            entries.push((child, file_type, name));
        }
        state.next_fh += 1;
        let fh = state.next_fh;
        state.listings.insert(fh, entries);
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let state = self.lock();
        let Some(entries) = state.listings.get(&fh.0) else {
            return reply.error(Errno::EBADF);
        };
        for (index, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*ino), index as u64 + 1, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.lock().listings.remove(&fh.0);
        reply.ok();
    }

    fn open(
        &self,
        _req: &Request,
        ino: INodeNo,
        flags: OpenFlags,
        _kill_suid_gid: bool,
        reply: ReplyOpen,
    ) {
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            return reply.error(Errno::EROFS);
        }
        let mut state = self.lock();
        let Some(Node {
            path,
            kind: Kind::File { size, .. },
        }) = state.nodes.get(&ino.0).cloned()
        else {
            return reply.error(Errno::EISDIR);
        };
        state.next_fh += 1;
        let fh = state.next_fh;
        state.readers.insert(
            fh,
            Reader {
                key: self.key(&path),
                size,
                start: 0,
                data: Bytes::new(),
            },
        );
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let (key, file_size, held) = {
            let state = self.lock();
            let Some(reader) = state.readers.get(&fh.0) else {
                return reply.error(Errno::EBADF);
            };
            // Read past its size as opened (the object may have grown since):
            // its end.
            if offset >= reader.size {
                return reply.data(&[]);
            }
            let end = offset.saturating_add(u64::from(size)).min(reader.size);
            let held = (offset >= reader.start && end <= reader.start + reader.data.len() as u64)
                .then(|| {
                    reader
                        .data
                        .slice((offset - reader.start) as usize..(end - reader.start) as usize)
                });
            (reader.key.clone(), reader.size, held)
        };
        if let Some(data) = held {
            return reply.data(&data);
        }
        let last = offset
            .saturating_add(u64::from(size).max(READ_AHEAD))
            .min(file_size)
            - 1;
        let fetched = self.runtime.block_on(self.client.get_object_range(
            &self.options.bucket,
            &key,
            offset,
            last,
        ));
        let data = match fetched {
            Ok(data) => data,
            Err(err) => return reply.error(errno(&err)),
        };
        let end = (u64::from(size) as usize).min(data.len());
        reply.data(&data[..end]);
        if let Some(reader) = self.lock().readers.get_mut(&fh.0) {
            reader.start = offset;
            reader.data = data;
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.lock().readers.remove(&fh.0);
        reply.ok();
    }
}

/// The session's configuration of `options`.
fn config(options: &Options) -> Config {
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName(format!("truthdb-mount:{}", options.bucket)),
        MountOption::Subtype("truthdb".to_string()),
        MountOption::RO,
        MountOption::NoDev,
        MountOption::NoSuid,
        MountOption::DefaultPermissions,
    ];
    if options.allow_other {
        config.acl = SessionACL::All;
    }
    // Requests are served by threads of their own: one waiting on the
    // service does not hold up the others.
    config.n_threads = Some(THREADS);
    config
}

/// The threads requests are served by.
const THREADS: usize = 4;

/// The bucket mounted at `directory`: [`fuser::Session::run`] serves it
/// until it is unmounted (its `unmount_callable` unmounts it).
pub fn session(
    client: Client,
    options: Options,
    directory: &Path,
) -> std::io::Result<fuser::Session<Mount>> {
    let config = config(&options);
    fuser::Session::new(Mount::new(client, options)?, directory, &config)
}

/// Mounts the bucket at `directory` in the background: unmounted when the
/// session returned is dropped.
pub fn spawn(
    client: Client,
    options: Options,
    directory: &Path,
) -> std::io::Result<fuser::BackgroundSession> {
    let config = config(&options);
    fuser::spawn_mount(Mount::new(client, options)?, directory, &config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_shown_as_mountpoint_shows_them() {
        for name in ["a", "a.txt", ".hidden", "with space"] {
            assert!(shown(name), "{name}");
        }
        for name in ["", ".", "..", "a\0b", "a/b"] {
            assert!(!shown(name), "{name:?}");
        }
        assert_eq!(child("", "a"), "a");
        assert_eq!(child("a/b", "c"), "a/b/c");
    }
}
