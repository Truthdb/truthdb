//! A bucket (or a prefix of one) as a local file system, with Mountpoint
//! for Amazon S3's semantics.
//!
//! Reading: directories are what `/` divides the keys into; a key ending in
//! `/` is no file (a directory of that name is); names `.`, `..`, empty
//! ones and those holding NUL are not shown; a file and a directory of one
//! name: the directory is. Reads are ranged GETs, read ahead. Files are
//! `0644` and directories `0755`, the mounting user's, unless set
//! otherwise; inode numbers last as long as the mount.
//!
//! Writing (unless mounted read-only): a new file is written from its
//! start, in order, and uploaded as it is (in parts, a multipart upload);
//! it is in the bucket once closed (or synced: no more is written to it
//! then), and is not read while written. An existing file is replaced only
//! with `allow_overwrite`, opened to be truncated; deleted only with
//! `allow_delete`. A directory made is the mount's own until a file is
//! written in it; only such a directory, empty, is removed. Renames,
//! links, and changes of mode or owner are refused.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo,
    InitFlags, KernelConfig, MountOption, OpenAccMode, OpenFlags, RenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
    SessionACL, TimeOrNow, WriteFlags,
};
use truthdb_s3client::{Client, Error};

/// How long the kernel keeps what it was told of a file or a directory
/// (Mountpoint: "stale metadata for that object for up to 1 second").
const TTL: Duration = Duration::from_secs(1);
/// What one read fetches at least: the reads after it in the file are
/// answered from it.
const READ_AHEAD: u64 = 4 * 1024 * 1024;
/// A file written is uploaded in parts of this size (the last smaller).
const PART: usize = 8 * 1024 * 1024;

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
    /// Nothing is written.
    pub read_only: bool,
    /// Files may be deleted.
    pub allow_delete: bool,
    /// Existing files may be replaced (opened to be truncated).
    pub allow_overwrite: bool,
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
    path: String,
    key: String,
    size: u64,
    start: u64,
    data: Bytes,
}

/// A file open for writing, as the mount's state has it.
struct Writer {
    path: String,
    key: String,
    /// Bytes written so far.
    written: u64,
    /// Uploaded whole (closed or synced): nothing more is written.
    done: bool,
    /// What failed, if anything: answered from then on.
    failed: Option<Errno>,
    /// The process that opened it: a close by another (a child of it,
    /// closing what it inherited) does not end it.
    opener: u32,
    /// Closed by its opener with nothing written: ended by its release, or
    /// by a newer open of the file (which then has it, as Mountpoint's
    /// "following Release or (a newer) Open").
    flushed: bool,
    /// What is uploaded of it: locked by each write, flush, sync and
    /// release of it, for all of it (its uploads too), so that they come
    /// one at a time; locked before the state, never while it is held.
    upload: Arc<Mutex<Upload>>,
}

/// What a file being written has not uploaded, and its multipart upload.
#[derive(Default)]
struct Upload {
    pending: Vec<u8>,
    /// Once a part is sent: its id and parts.
    multipart: Option<(String, Vec<(u32, String)>)>,
}

#[derive(Default)]
struct State {
    nodes: HashMap<u64, Node>,
    by_path: HashMap<String, u64>,
    next_ino: u64,
    /// Directories open: their entries (inode, kind, name), as listed.
    listings: HashMap<u64, Vec<(u64, FileType, String)>>,
    readers: HashMap<u64, Reader>,
    writers: HashMap<u64, Writer>,
    /// Directories made here, not (yet) in the bucket.
    local_directories: HashSet<String>,
    /// Files being deleted: not opened to be written meanwhile.
    deleting: HashSet<String>,
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

    fn handle(&mut self) -> u64 {
        self.next_fh += 1;
        self.next_fh
    }

    /// The writer of `path` still writing, if one is.
    fn writing(&self, path: &str) -> Option<&Writer> {
        self.writers
            .values()
            .find(|writer| writer.path == path && !writer.done)
    }

    fn reading(&self, path: &str) -> bool {
        self.readers.values().any(|reader| reader.path == path)
    }

    /// What is here of `path`: a file being written, or a directory made.
    fn local(&self, path: &str) -> Option<Kind> {
        if let Some(writer) = self.writing(path) {
            return Some(Kind::File {
                size: writer.written,
                mtime: now(),
            });
        }
        self.local_directories
            .contains(path)
            .then_some(Kind::Directory)
    }
}

/// The process (thread group) of thread `pid` (a request's pid is its
/// thread's); `pid` itself when that cannot be read.
fn process_of(pid: u32) -> u32 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Tgid:"))
                .and_then(|tgid| tgid.trim().parse().ok())
        })
        .unwrap_or(pid)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
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

fn child(parent: &str, name: &str) -> String {
    match parent {
        "" => name.to_string(),
        parent => format!("{parent}/{name}"),
    }
}

/// What a remote path is.
enum Found {
    Directory,
    File { size: u64, mtime: u64 },
    Nothing,
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

    /// A path's key (a file's).
    fn key(&self, path: &str) -> String {
        format!("{}{path}", self.options.prefix)
    }

    /// A path's prefix (a directory's: ending in `/`).
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

    /// What `path` is in the bucket: a directory (a key under it), else a
    /// file (its key), else nothing.
    fn find_remote(&self, path: &str) -> Result<Found, Error> {
        let bucket = &self.options.bucket;
        self.runtime.block_on(async {
            let under = self
                .client
                .list_objects(bucket, &self.directory_prefix(path), Some("/"), None)
                .await?;
            if !under.objects.is_empty() || !under.prefixes.is_empty() {
                return Ok(Found::Directory);
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
                .map_or(Found::Nothing, |object| Found::File {
                    size: object.size,
                    mtime: truthdb_s3client::iso_seconds(&object.last_modified).unwrap_or(0),
                }))
        })
    }

    /// What `path` is: "remote directories will always shadow local
    /// directories or files", which shadow remote files.
    fn find(&self, path: &str) -> Result<Option<Kind>, Error> {
        let remote = self.find_remote(path)?;
        if let Found::Directory = remote {
            return Ok(Some(Kind::Directory));
        }
        if let Some(local) = self.lock().local(path) {
            return Ok(Some(local));
        }
        Ok(match remote {
            Found::File { size, mtime } => Some(Kind::File { size, mtime }),
            _ => None,
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
                    // (A service answering what was not asked for: skipped.)
                    let Some(rest) = common.strip_prefix(prefix.as_str()) else {
                        continue;
                    };
                    let name = rest.trim_end_matches('/');
                    if shown(name) && rest.ends_with('/') && name.len() + 1 == rest.len() {
                        directories.push(name.to_string());
                    }
                }
                for object in page.objects {
                    let Some(name) = object.key.strip_prefix(prefix.as_str()) else {
                        continue;
                    };
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
        // What is here: directories made, files being written.
        {
            let state = self.lock();
            let below = |other: &str| {
                let rest = match path {
                    "" => Some(other),
                    path => other
                        .strip_prefix(path)
                        .and_then(|rest| rest.strip_prefix('/')),
                };
                rest.filter(|name| !name.is_empty() && !name.contains('/'))
                    .map(str::to_string)
            };
            for made in &state.local_directories {
                if let Some(name) = below(made)
                    && !directories.contains(&name)
                {
                    directories.push(name);
                }
            }
            for writer in state.writers.values().filter(|writer| !writer.done) {
                if let Some(name) = below(&writer.path) {
                    files.retain(|(listed, _)| *listed != name);
                    files.push((
                        name,
                        Kind::File {
                            size: writer.written,
                            mtime: now(),
                        },
                    ));
                }
            }
        }
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

    /// The path of the entry `name` of directory `parent`, if both are.
    fn entry_path(&self, parent: INodeNo, name: &OsStr) -> Option<String> {
        let name = name.to_str().filter(|name| shown(name))?;
        let parent = self.lock().nodes.get(&parent.0)?.path.clone();
        Some(child(&parent, name))
    }

    /// Opens `path` to be written (as a new file, or replacing one), if it
    /// is neither written nor read: the check and the opening in one hold
    /// of the state, so that two opens at once do not both have it. `pid`
    /// is the opening thread's.
    fn writer(&self, path: &str, pid: u32) -> Result<u64, Errno> {
        self.settle(path);
        let mut state = self.lock();
        if state.writing(path).is_some() || state.reading(path) || state.deleting.contains(path) {
            return Err(Errno::EPERM);
        }
        let fh = state.handle();
        state.writers.insert(
            fh,
            Writer {
                path: path.to_string(),
                key: self.key(path),
                written: 0,
                done: false,
                failed: None,
                opener: process_of(pid),
                flushed: false,
                upload: Arc::default(),
            },
        );
        Ok(fh)
    }

    /// Ends the writers of `path` closed with nothing written, as a newer
    /// open (or a delete) of it comes: their descriptors, if still open
    /// (duplicated), write no more. Called before the state is held.
    fn settle(&self, path: &str) {
        let closed: Vec<(u64, Arc<Mutex<Upload>>)> = self
            .lock()
            .writers
            .iter()
            .filter(|(_, writer)| writer.path == path && writer.flushed && !writer.done)
            .map(|(fh, writer)| (*fh, Arc::clone(&writer.upload)))
            .collect();
        for (fh, upload) in closed {
            let mut upload = upload
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _ = self.upload(fh, &mut upload, true);
        }
    }

    /// Writer `fh`'s upload, to be locked.
    fn upload_of(&self, fh: u64) -> Option<Arc<Mutex<Upload>>> {
        self.lock()
            .writers
            .get(&fh)
            .map(|writer| Arc::clone(&writer.upload))
    }

    /// Uploads writer `fh`'s full parts (with `last`: all of it, and ends
    /// its upload); `upload` is its upload, locked by the caller.
    fn upload(&self, fh: u64, upload: &mut Upload, last: bool) -> Result<(), Errno> {
        let key = {
            let state = self.lock();
            let writer = state.writers.get(&fh).ok_or(Errno::EBADF)?;
            if let Some(failed) = writer.failed {
                return Err(failed);
            }
            if writer.done || (!last && upload.pending.len() < PART) {
                return Ok(());
            }
            writer.key.clone()
        };
        let bucket = &self.options.bucket;
        let sent = self.runtime.block_on(async {
            // A file of one part (or none) is put whole.
            if last && upload.multipart.is_none() && upload.pending.len() <= PART {
                let body = Bytes::from(std::mem::take(&mut upload.pending));
                self.client.put_object(bucket, &key, &[], body).await?;
                return Ok::<_, Error>(());
            }
            if upload.multipart.is_none() {
                let id = self
                    .client
                    .create_multipart_upload(bucket, &key, &[])
                    .await?;
                upload.multipart = Some((id, Vec::new()));
            }
            let Upload { pending, multipart } = upload;
            let (id, parts) = multipart.as_mut().expect("just begun");
            while pending.len() >= PART || (last && !pending.is_empty()) {
                let rest = pending.split_off(PART.min(pending.len()));
                let number = parts.len() as u32 + 1;
                let part = Bytes::from(std::mem::replace(pending, rest));
                let etag = self
                    .client
                    .upload_part(bucket, &key, id, number, part)
                    .await?;
                parts.push((number, etag));
            }
            if last {
                self.client
                    .complete_multipart_upload(bucket, &key, id, parts)
                    .await?;
            }
            Ok(())
        });
        if sent.is_err()
            && let Some((id, _)) = upload.multipart.take()
        {
            // Not to be finished: its parts are dropped.
            let _ = self
                .runtime
                .block_on(self.client.abort_multipart_upload(bucket, &key, &id));
        }
        let mut state = self.lock();
        let Some(writer) = state.writers.get_mut(&fh) else {
            return Err(Errno::EBADF);
        };
        match sent {
            Ok(()) => {
                if last {
                    writer.done = true;
                    let (path, size) = (writer.path.clone(), writer.written);
                    state.inode(&path, Kind::File { size, mtime: now() });
                }
                Ok(())
            }
            Err(err) => {
                let failed = errno(&err);
                writer.failed = Some(failed);
                Err(failed)
            }
        }
    }
}

impl Filesystem for Mount {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // An open that truncates says so (rather than truncating first).
        let _ = config.add_capabilities(InitFlags::FUSE_ATOMIC_O_TRUNC);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(path) = self.entry_path(parent, name) else {
            return reply.error(Errno::ENOENT);
        };
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
        let state = self.lock();
        let Some(node) = state.nodes.get(&ino.0).cloned() else {
            return reply.error(Errno::ENOENT);
        };
        let kind = state.local(&node.path).unwrap_or(node.kind);
        drop(state);
        reply.attr(&TTL, &self.attr(ino.0, &kind));
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        _kill_suid_gid: bool,
        reply: ReplyAttr,
    ) {
        if self.options.read_only {
            return reply.error(Errno::EROFS);
        }
        // "Modifying file metadata (`chmod`, `chown`, `chgrp`) is not
        // supported."
        if mode.is_some() || uid.is_some() || gid.is_some() {
            return reply.error(Errno::EPERM);
        }
        let state = self.lock();
        let Some(node) = state.nodes.get(&ino.0).cloned() else {
            return reply.error(Errno::ENOENT);
        };
        let writer = fh
            .and_then(|fh| state.writers.get(&fh.0))
            .or_else(|| state.writing(&node.path));
        // A size is set only as a file being written has it (truncating a
        // file just made, as an open that truncates does); times only of a
        // file being written ("utime ... is supported only on files that
        // are being written"), and kept as its upload has them.
        let ok = match (writer, size) {
            (Some(writer), Some(size)) => size == writer.written,
            (Some(_), None) => true,
            (None, _) => false,
        };
        if !ok {
            return reply.error(Errno::EPERM);
        }
        let kind = state.local(&node.path).unwrap_or(node.kind);
        drop(state);
        reply.attr(&TTL, &self.attr(ino.0, &kind));
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _owner: fuser::Owner,
        reply: ReplyEntry,
    ) {
        if self.options.read_only {
            return reply.error(Errno::EROFS);
        }
        let Some(path) = self.entry_path(parent, name) else {
            return reply.error(Errno::EINVAL);
        };
        match self.find(&path) {
            Ok(Some(_)) => return reply.error(Errno::EEXIST),
            Ok(None) => {}
            Err(err) => return reply.error(errno(&err)),
        }
        let mut state = self.lock();
        state.local_directories.insert(path.clone());
        let ino = state.inode(&path, Kind::Directory);
        drop(state);
        reply.entry(&TTL, &self.attr(ino, &Kind::Directory), Generation(0));
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.options.read_only {
            return reply.error(Errno::EROFS);
        }
        let Some(path) = self.entry_path(parent, name) else {
            return reply.error(Errno::ENOENT);
        };
        // "`rmdir` will only delete empty directories created by `mkdir`."
        if !self.lock().local_directories.contains(&path) {
            return match self.find(&path) {
                Ok(Some(Kind::Directory)) => reply.error(Errno::EPERM),
                Ok(Some(_)) => reply.error(Errno::ENOTDIR),
                Ok(None) => reply.error(Errno::ENOENT),
                Err(err) => reply.error(errno(&err)),
            };
        }
        match self.list(&path) {
            Ok(listed) if listed.is_empty() => {}
            Ok(_) => return reply.error(Errno::ENOTEMPTY),
            Err(err) => return reply.error(errno(&err)),
        }
        let mut state = self.lock();
        state.local_directories.remove(&path);
        if let Some(ino) = state.by_path.remove(&path) {
            state.nodes.remove(&ino);
        }
        reply.ok();
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        if self.options.read_only {
            return reply.error(Errno::EROFS);
        }
        // "By default, Mountpoint does not allow deleting existing objects".
        if !self.options.allow_delete {
            return reply.error(Errno::EPERM);
        }
        let Some(path) = self.entry_path(parent, name) else {
            return reply.error(Errno::ENOENT);
        };
        // "For files not yet committed to S3 ... the client does not
        // permit `unlink` operations." Checked, and the file marked as being
        // deleted, in one hold of the state: one opened to be written
        // meanwhile is refused until the delete is done.
        self.settle(&path);
        {
            let mut state = self.lock();
            if state.writing(&path).is_some() || !state.deleting.insert(path.clone()) {
                return reply.error(Errno::EPERM);
            }
        }
        let deleted = match self.find(&path) {
            Ok(Some(Kind::File { .. })) => self
                .runtime
                .block_on(
                    self.client
                        .delete_object(&self.options.bucket, &self.key(&path)),
                )
                .map_err(|err| errno(&err)),
            Ok(Some(Kind::Directory)) => Err(Errno::EISDIR),
            Ok(None) => Err(Errno::ENOENT),
            Err(err) => Err(errno(&err)),
        };
        let mut state = self.lock();
        state.deleting.remove(&path);
        match deleted {
            Ok(()) => {
                if let Some(ino) = state.by_path.remove(&path) {
                    state.nodes.remove(&ino);
                }
                reply.ok();
            }
            Err(err) => reply.error(err),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _newparent: INodeNo,
        _newname: &OsStr,
        _flags: RenameFlags,
        _owner: Option<fuser::Owner>,
        reply: ReplyEmpty,
    ) {
        // "Attempting to rename files where unsupported by S3 will result
        // in the operation being rejected." (A general purpose bucket.)
        reply.error(if self.options.read_only {
            Errno::EROFS
        } else {
            Errno::ENOTSUP
        });
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        _kill_suid_gid: bool,
        _owner: fuser::Owner,
        reply: ReplyCreate,
    ) {
        if self.options.read_only {
            return reply.error(Errno::EROFS);
        }
        let Some(path) = self.entry_path(parent, name) else {
            return reply.error(Errno::EINVAL);
        };
        self.settle(&path);
        {
            let state = self.lock();
            if state.writing(&path).is_some() || state.reading(&path) {
                return reply.error(Errno::EPERM);
            }
        }
        match self.find(&path) {
            Ok(None) => {}
            Ok(Some(Kind::Directory)) => return reply.error(Errno::EISDIR),
            Ok(Some(Kind::File { .. })) => {
                if flags & libc::O_EXCL != 0 {
                    return reply.error(Errno::EEXIST);
                }
                if !self.options.allow_overwrite || flags & libc::O_TRUNC == 0 {
                    return reply.error(Errno::EPERM);
                }
            }
            Err(err) => return reply.error(errno(&err)),
        }
        let fh = match self.writer(&path, req.pid()) {
            Ok(fh) => fh,
            Err(err) => return reply.error(err),
        };
        let kind = Kind::File {
            size: 0,
            mtime: now(),
        };
        let ino = self.lock().inode(&path, kind.clone());
        reply.created(
            &TTL,
            &self.attr(ino, &kind),
            Generation(0),
            FileHandle(fh),
            FopenFlags::empty(),
        );
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
        let fh = state.handle();
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
        req: &Request,
        ino: INodeNo,
        flags: OpenFlags,
        _kill_suid_gid: bool,
        reply: ReplyOpen,
    ) {
        let Some(node) = self.lock().nodes.get(&ino.0).cloned() else {
            return reply.error(Errno::ENOENT);
        };
        let Kind::File { size, .. } = node.kind else {
            return reply.error(Errno::EISDIR);
        };
        if flags.acc_mode() != OpenAccMode::O_RDONLY {
            if self.options.read_only {
                return reply.error(Errno::EROFS);
            }
            // An existing file is replaced only so ("with O_TRUNC", and
            // `allow_overwrite`).
            if !self.options.allow_overwrite || flags.0 & libc::O_TRUNC == 0 {
                return reply.error(Errno::EPERM);
            }
            let fh = match self.writer(&node.path, req.pid()) {
                Ok(fh) => fh,
                Err(err) => return reply.error(err),
            };
            return reply.opened(FileHandle(fh), FopenFlags::empty());
        }
        self.settle(&node.path);
        let mut state = self.lock();
        // "a file cannot have a reader and a writer open at the same time".
        if state.writing(&node.path).is_some() {
            return reply.error(Errno::EPERM);
        }
        let fh = state.handle();
        state.readers.insert(
            fh,
            Reader {
                path: node.path.clone(),
                key: self.key(&node.path),
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

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyWrite,
    ) {
        let Some(upload) = self.upload_of(fh.0) else {
            return reply.error(Errno::EBADF);
        };
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        {
            let mut state = self.lock();
            let Some(writer) = state.writers.get_mut(&fh.0) else {
                return reply.error(Errno::EBADF);
            };
            if let Some(failed) = writer.failed {
                return reply.error(failed);
            }
            // Synced: no more ("disallow further writes").
            if writer.done {
                return reply.error(Errno::EBADF);
            }
            // "All writes must be sequential".
            if offset != writer.written {
                return reply.error(Errno::EINVAL);
            }
            writer.written += data.len() as u64;
            writer.flushed = false;
        }
        upload.pending.extend_from_slice(data);
        match self.upload(fh.0, &mut upload, false) {
            Ok(()) => reply.written(data.len() as u32),
            Err(err) => reply.error(err),
        }
    }

    fn flush(
        &self,
        req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        // Nothing written yet: not ended ("only completing the upload on
        // flush when some bytes have been written", as Mountpoint does: a
        // shell's `>` and `dd` duplicate the descriptor and close the first
        // before writing; `touch` closes, then sets the times by path).
        {
            let mut state = self.lock();
            if let Some(writer) = state.writers.get_mut(&fh.0)
                && writer.written == 0
            {
                if writer.opener == process_of(req.pid()) {
                    writer.flushed = true;
                }
                return reply.ok();
            }
        }
        // A file written is uploaded whole as it is closed: by its opener
        // (another's close, as a child's of what it inherited, returns at
        // once: "if close is called by a different process than the one
        // that opened it, close returns immediately"; it is uploaded when
        // the last is closed).
        let opener = self.lock().writers.get(&fh.0).map(|writer| writer.opener);
        if opener.is_some_and(|opener| opener != process_of(req.pid())) {
            return reply.ok();
        }
        let Some(upload) = self.upload_of(fh.0) else {
            return reply.ok();
        };
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.upload(fh.0, &mut upload, true) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let Some(upload) = self.upload_of(fh.0) else {
            return reply.ok();
        };
        let mut upload = upload
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // "Synchronization operations ... complete the upload of the object
        // to S3 and disallow further writes."
        match self.upload(fh.0, &mut upload, true) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
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
        // A writer not flushed (its last close not seen as such) is
        // uploaded now.
        if let Some(upload) = self.upload_of(fh.0) {
            let mut upload = upload
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _ = self.upload(fh.0, &mut upload, true);
        }
        let mut state = self.lock();
        state.readers.remove(&fh.0);
        state.writers.remove(&fh.0);
        reply.ok();
    }
}

/// The session's configuration of `options`.
fn config(options: &Options) -> Config {
    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName(format!("truthdb-mount:{}", options.bucket)),
        MountOption::Subtype("truthdb".to_string()),
        if options.read_only {
            MountOption::RO
        } else {
            MountOption::RW
        },
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
