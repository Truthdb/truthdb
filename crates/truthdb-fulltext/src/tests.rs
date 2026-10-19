//! The index on a real store: what is indexed is found and ranked, what is
//! unindexed or rolled back leaves nothing behind, and a search read in
//! small slices answers as one read whole.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use truthdb_core::relstore::btree::{BTree, TreeRoots};
use truthdb_core::relstore::ctx::{OpMode, RelCtx, TxnLink};
use truthdb_core::relstore::recovery::{LogicalUndo, rollback, rollback_to};
use truthdb_core::storage::{
    CATALOG_ENGINE_ID, EngineParticipant, Storage, StorageError, StorageFile, StorageOptions,
};
use truthdb_core::wal::records::{PageOpUndo, RelRecord};

use crate::{DocTerms, Hit, Index, MAX_PREFIX_TERMS, Search, SearchError, Stats};

const POSTINGS: u32 = 0xE000_0001;
const DOCS: u32 = 0xE000_0002;

/// The catalog slot with nothing in it: these tests own every tree.
struct NoCatalog;

struct NoUndo;

impl LogicalUndo for NoUndo {
    fn undo(
        &self,
        _ctx: &mut RelCtx<'_>,
        _mode: &mut OpMode<'_>,
        object_id: u32,
        _undo: &PageOpUndo,
    ) -> Result<(), StorageError> {
        Err(StorageError::InvalidFile(format!("no tree {object_id}")))
    }
}

impl EngineParticipant for NoCatalog {
    fn engine_id(&self) -> u32 {
        CATALOG_ENGINE_ID
    }
    fn owns_object(&self, _object_id: u32) -> bool {
        false
    }
    fn open(&mut self, _metadata_root: Option<u64>, _db_options: u8) {}
    fn metadata_root(&self) -> Option<u64> {
        None
    }
    fn set_metadata_root(&mut self, _page: u64) {}
    fn options_byte(&self) -> u8 {
        0
    }
    fn reload(&mut self, _core: &mut StorageFile) -> Result<(), StorageError> {
        Ok(())
    }
    fn recovery_undo(&self) -> Box<dyn LogicalUndo> {
        Box::new(NoUndo)
    }
    fn capture_standby(
        &mut self,
        _core: &mut StorageFile,
        _records: &[(u64, RelRecord)],
        _floor: u64,
    ) {
    }
    fn finish_recovery(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn temp_path(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("truthdb-fulltext-{label}-{nanos}.db"))
}

struct Fixture {
    path: PathBuf,
    storage: Storage,
    index: Index,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn fixture(label: &str) -> Fixture {
    let path = temp_path(label);
    let storage = create(&path);
    let index = {
        let mut guard = storage.lock();
        let mut ctx = guard.page_ctx(0);
        Index {
            postings: BTree::create(&mut ctx, POSTINGS).expect("postings tree"),
            docs: BTree::create(&mut ctx, DOCS).expect("docs tree"),
        }
    };
    Fixture {
        path,
        storage,
        index,
    }
}

fn create(path: &Path) -> Storage {
    Storage::create_with_wal_bounds(
        path.to_path_buf(),
        StorageOptions {
            size_gib: 1,
            wal_ratio: 0.05,
            metadata_ratio: 0.08,
            snapshot_ratio: 0.02,
            allocator_ratio: 0.02,
            reserved_ratio: 0.17,
            default_collation: None,
        },
        16 * 1024 * 1024,
        16 * 1024 * 1024,
        || vec![Box::new(NoCatalog) as Box<dyn EngineParticipant>],
    )
    .expect("create")
}

impl Fixture {
    /// Runs `body` in one transaction, committed when `commit`, else rolled
    /// back.
    fn txn<T>(
        &self,
        commit: bool,
        body: impl FnOnce(&mut RelCtx<'_>, &mut TxnLink, &Index) -> T,
    ) -> T {
        let mut guard = self.storage.lock();
        let txn_id = guard.alloc_txn_id();
        let out = {
            let mut ctx = guard.page_ctx(0);
            let mut txn = ctx.begin(txn_id).expect("begin");
            let out = body(&mut ctx, &mut txn, &self.index);
            if commit {
                ctx.commit(txn).expect("commit");
            } else {
                let roots = TreeRoots(HashMap::from([
                    (POSTINGS, self.index.postings.root),
                    (DOCS, self.index.docs.root),
                ]));
                rollback(&mut ctx, txn, &roots).expect("rollback");
            }
            out
        };
        guard.wal_sync_all().expect("sync");
        out
    }

    fn index(&self, scope: &str, doc: u64, text: &str) {
        self.txn(true, |ctx, txn, index| {
            index
                .index_document(
                    ctx,
                    txn,
                    scope.as_bytes(),
                    doc,
                    &DocTerms::from_text(text),
                    u64::MAX,
                )
                .expect("index")
        });
    }

    fn unindex(&self, scope: &str, doc: u64) -> bool {
        self.txn(true, |ctx, txn, index| {
            index
                .unindex_document(ctx, txn, scope.as_bytes(), doc)
                .expect("unindex")
        })
    }

    fn stats(&self, scope: &str) -> Stats {
        let mut guard = self.storage.lock();
        self.index
            .stats(&mut guard.page_ctx(0), scope.as_bytes())
            .expect("stats")
    }

    /// Every row of both trees.
    fn rows(&self) -> usize {
        let mut guard = self.storage.lock();
        let mut ctx = guard.page_ctx(0);
        self.index.postings.scan(&mut ctx).expect("scan").len()
            + self.index.docs.scan(&mut ctx).expect("scan").len()
    }

    /// The search's hits, read `budget` postings per lock hold.
    fn search_in(
        &self,
        scope: &str,
        condition: &str,
        budget: usize,
    ) -> Result<Vec<Hit>, SearchError> {
        let mut search = Search::new(condition)?;
        loop {
            let mut guard = self.storage.lock();
            let mut ctx = guard.page_ctx(0);
            if search.step(&mut ctx, &self.index, scope.as_bytes(), budget)? {
                let stats = self.index.stats(&mut ctx, scope.as_bytes())?;
                return Ok(search.rank(stats));
            }
        }
    }

    fn docs(&self, scope: &str, condition: &str) -> Vec<u64> {
        self.search_in(scope, condition, 1000)
            .expect("search")
            .iter()
            .map(|hit| hit.doc)
            .collect()
    }
}

fn sorted(mut docs: Vec<u64>) -> Vec<u64> {
    docs.sort();
    docs
}

#[test]
fn conditions_find_the_documents_that_satisfy_them() {
    let f = fixture("conditions");
    f.index("b", 1, "The quick brown fox jumps over the lazy dog");
    f.index("b", 2, "A quick brown cat");
    f.index("b", 3, "Foxes and dogs; a database of databases");
    f.index("b", 4, "東京タワー");
    assert_eq!(sorted(f.docs("b", "quick")), [1, 2]);
    assert_eq!(sorted(f.docs("b", "QUICK and fox")), [1]);
    assert_eq!(sorted(f.docs("b", "fox or cat")), [1, 2]);
    assert_eq!(sorted(f.docs("b", "quick and not fox")), [2]);
    assert_eq!(sorted(f.docs("b", "\"fox*\"")), [1, 3]);
    assert_eq!(sorted(f.docs("b", "\"data*\" or \"do*\"")), [1, 3]);
    assert_eq!(sorted(f.docs("b", "\"brown cat\"")), [2]);
    assert_eq!(sorted(f.docs("b", "(fox | cat) &! lazy")), [2]);
    assert_eq!(sorted(f.docs("b", "東京")), [4]);
    assert_eq!(sorted(f.docs("b", "タワー")), [4]);
    assert!(f.docs("b", "zebra").is_empty());
    assert!(f.docs("other", "quick").is_empty());
}

#[test]
fn rarer_terms_and_denser_documents_rank_first() {
    let f = fixture("ranking");
    // "apple" is in every document, "kiwi" in one: a match on kiwi outranks
    // one on apple. Among apple matches, more apples in a shorter document
    // rank higher.
    f.index("b", 10, "apple apple apple");
    f.index("b", 11, "apple pear plum grape melon lime lemon");
    f.index("b", 12, "apple kiwi pear plum grape melon lime");
    let hits = f.search_in("b", "apple or kiwi", 1000).unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.doc).collect::<Vec<_>>(),
        [12, 10, 11]
    );
    assert!(hits.windows(2).all(|pair| pair[0].score > pair[1].score));
    // Equal scores rank by document.
    f.index("b", 13, "apple pear plum grape melon lime lemon");
    let hits = f.docs("b", "lemon");
    assert_eq!(hits, [11, 13]);
}

#[test]
fn reindexing_replaces_and_unindexing_removes_everything() {
    let f = fixture("unindex");
    f.index("b", 1, "red green blue");
    f.index("b", 2, "red red");
    assert_eq!(
        f.stats("b"),
        Stats {
            docs: 2,
            total_len: 5
        }
    );
    f.index("b", 1, "yellow");
    assert_eq!(sorted(f.docs("b", "red")), [2]);
    assert_eq!(sorted(f.docs("b", "yellow")), [1]);
    assert!(f.docs("b", "green").is_empty());
    assert_eq!(
        f.stats("b"),
        Stats {
            docs: 2,
            total_len: 3
        }
    );
    assert!(f.unindex("b", 1));
    assert!(!f.unindex("b", 1));
    assert!(f.unindex("b", 2));
    assert_eq!(f.stats("b"), Stats::default());
    assert_eq!(f.rows(), 0);
}

#[test]
fn a_document_with_many_terms_spans_chunks_and_unindexes_whole() {
    let f = fixture("chunks");
    let text: String = (0..5000).map(|n| format!("term{n} ")).collect();
    f.index("b", 7, &text);
    f.index("b", 8, "term42 other");
    {
        let mut guard = f.storage.lock();
        let docs = f.index.docs.scan(&mut guard.page_ctx(0)).unwrap();
        // Stats, two documents, the first in many chunks.
        assert!(docs.len() > 20, "{} document rows", docs.len());
    }
    assert_eq!(sorted(f.docs("b", "term4999")), [7]);
    assert_eq!(sorted(f.docs("b", "term42")), [7, 8]);
    assert!(f.unindex("b", 7));
    assert_eq!(sorted(f.docs("b", "term42")), [8]);
    assert!(f.docs("b", "term4999").is_empty());
    assert!(f.unindex("b", 8));
    assert_eq!(f.rows(), 0);
}

#[test]
fn a_rolled_back_index_leaves_nothing() {
    let f = fixture("rollback");
    f.index("b", 1, "kept words");
    f.txn(false, |ctx, txn, index| {
        index
            .index_document(
                ctx,
                txn,
                b"b",
                2,
                &DocTerms::from_text("lost words"),
                u64::MAX,
            )
            .unwrap();
        index.unindex_document(ctx, txn, b"b", 1).unwrap();
    });
    assert_eq!(sorted(f.docs("b", "words")), [1]);
    assert!(f.docs("b", "lost").is_empty());
    assert_eq!(
        f.stats("b"),
        Stats {
            docs: 1,
            total_len: 2
        }
    );
}

#[test]
fn scopes_are_separate_collections() {
    let f = fixture("scopes");
    f.index("a", 1, "shared alpha");
    f.index("ab", 2, "shared beta");
    f.index("", 3, "shared gamma");
    assert_eq!(f.docs("a", "shared"), [1]);
    assert_eq!(f.docs("ab", "shared"), [2]);
    assert_eq!(f.docs("", "shared"), [3]);
    assert_eq!(f.stats("a").docs, 1);
    f.index(
        "a",
        9,
        &(0..2000).map(|n| format!("w{n} ")).collect::<String>(),
    );
    f.index("a", 4, "four");
    {
        let mut guard = f.storage.lock();
        let mut ctx = guard.page_ctx(0);
        assert_eq!(f.index.documents(&mut ctx, b"a", 10).unwrap(), [1, 4, 9]);
        assert_eq!(f.index.documents(&mut ctx, b"a", 2).unwrap(), [1, 4]);
        assert_eq!(f.index.documents(&mut ctx, b"ab", 10).unwrap(), [2]);
        assert!(f.index.documents(&mut ctx, b"zz", 10).unwrap().is_empty());
    }
    assert!(f.unindex("a", 4));
    assert!(f.unindex("a", 9));
    assert!(f.unindex("a", 1));
    assert_eq!(f.docs("ab", "shared"), [2]);
    // A scope longer than 255 bytes cannot be written.
    let long = "s".repeat(256);
    f.txn(false, |ctx, txn, index| {
        assert!(
            index
                .index_document(
                    ctx,
                    txn,
                    long.as_bytes(),
                    1,
                    &DocTerms::from_text("x"),
                    u64::MAX
                )
                .is_err()
        );
    });
}

#[test]
fn a_search_read_in_small_slices_answers_as_one_read_whole() {
    let f = fixture("slices");
    for doc in 0..300u64 {
        let text = format!(
            "common w{} {} {}",
            doc % 7,
            if doc % 3 == 0 { "fizz" } else { "" },
            if doc % 5 == 0 { "buzz" } else { "" }
        );
        f.index("b", doc, &text);
    }
    for condition in [
        "common",
        "fizz and buzz",
        "\"w*\" and not fizz",
        "w3 or buzz",
    ] {
        let whole = f.search_in("b", condition, 1_000_000).unwrap();
        for budget in [1, 2, 7, 100] {
            assert_eq!(
                f.search_in("b", condition, budget).unwrap(),
                whole,
                "{condition} by {budget}"
            );
        }
    }
    assert_eq!(f.docs("b", "fizz and buzz").len(), 20);
}

#[test]
fn a_prefix_matching_too_many_terms_is_refused() {
    let f = fixture("prefix-cap");
    let text: String = (0..=MAX_PREFIX_TERMS).map(|n| format!("p{n} ")).collect();
    f.index("b", 1, &text);
    assert!(matches!(
        f.search_in("b", "\"p*\"", 100),
        Err(SearchError::TooManyTerms(_))
    ));
    assert_eq!(f.docs("b", "\"p10*\""), [1]);
    assert!(matches!(
        f.search_in("b", "a b", 100),
        Err(SearchError::Query(_))
    ));
}

#[test]
fn terms_a_row_cannot_hold_are_refused_before_anything_is_written() {
    let f = fixture("bad-terms");
    for bad in [
        "x".repeat(300),
        "y".repeat(65),
        String::new(),
        "a\0b".to_string(),
    ] {
        let doc = DocTerms {
            counts: [("fine".to_string(), 1), (bad.clone(), 1)].into(),
            len: 2,
        };
        f.txn(true, |ctx, txn, index| {
            assert!(
                index
                    .index_document(ctx, txn, b"b", 1, &doc, u64::MAX)
                    .is_err(),
                "{bad:?}"
            );
        });
    }
    assert_eq!(f.rows(), 0);
}

#[test]
fn every_lookup_counts_against_the_slice_budget() {
    let f = fixture("budget");
    f.index("b", 1, "hit");
    let condition: String = std::iter::once("hit".to_string())
        .chain((0..200).map(|n| format!(" or miss{n}")))
        .collect();
    let mut search = Search::new(&condition).unwrap();
    let mut steps = 0;
    loop {
        steps += 1;
        let mut guard = f.storage.lock();
        if search
            .step(&mut guard.page_ctx(0), &f.index, b"b", 10)
            .unwrap()
        {
            break;
        }
    }
    // 201 lookups, at most 10 per step.
    assert!(steps >= 20, "{steps} steps");
}

#[test]
fn a_document_past_the_log_budget_stops_and_rolls_back_to_a_savepoint() {
    let f = fixture("budget-log");
    f.index("b", 1, "kept");
    let text: String = (0..5000).map(|n| format!("w{n} ")).collect();
    let roots = TreeRoots(HashMap::from([
        (POSTINGS, f.index.postings.root),
        (DOCS, f.index.docs.root),
    ]));
    f.txn(true, |ctx, txn, index| {
        let savepoint = txn.savepoint();
        let start = ctx.io.wal.tail();
        let done = index
            .index_document(ctx, txn, b"b", 2, &DocTerms::from_text(&text), 64 * 1024)
            .unwrap();
        assert!(
            !done,
            "five thousand postings write more than 64 KiB of log"
        );
        // Stopped close to the budget, not at the end of the document.
        assert!(ctx.io.wal.tail() - start < 256 * 1024);
        rollback_to(ctx, txn, savepoint, &roots).unwrap();
        // Within the budget it goes through.
        assert!(
            index
                .index_document(ctx, txn, b"b", 3, &DocTerms::from_text("small"), 64 * 1024)
                .unwrap()
        );
    });
    assert_eq!(f.docs("b", "kept"), [1]);
    assert_eq!(f.docs("b", "small"), [3]);
    assert!(f.docs("b", "w42").is_empty());
    assert_eq!(f.stats("b").docs, 2);
}
