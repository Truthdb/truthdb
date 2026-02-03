use std::path::PathBuf;

use crate::storage_layout::{FileHeader, Superblock, assert_layout_invariants};

pub struct Storage {
    file: StorageFile,
}

impl Storage {
    pub fn new(path: PathBuf) -> Self {
        assert_layout_invariants();
        let file = StorageFile::new(path);
        file.touch();
        Storage { file }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.file.path
    }
}

struct StorageFile {
    path: PathBuf,
    header: FileHeader,
    superblock_a: Superblock,
    superblock_b: Superblock,
}

impl StorageFile {
    fn new(path: PathBuf) -> Self {
        StorageFile {
            path,
            header: FileHeader::default(),
            superblock_a: Superblock::default(),
            superblock_b: Superblock::default(),
        }
    }

    fn touch(&self) {
        let _ = self.header.magic;
        let _ = self.superblock_a.generation;
        let _ = self.superblock_b.generation;
    }
}
