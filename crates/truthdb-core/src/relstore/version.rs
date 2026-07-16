//! Stage 13 version store: row versions for READ COMMITTED SNAPSHOT (and,
//! next, SNAPSHOT isolation), kept entirely in memory.
//!
//! Why in memory and not on rows or temp extents: the row codec is positional
//! with no per-row header (a version stamp would change the on-disk format for
//! every table), deletes are physical (a snapshot reader could never encounter
//! a deleted row's ghost), and — decisively — version state has no durability
//! requirement at all. A snapshot exists only inside a running process; after
//! a restart there are none, so every physical row is visible to every new
//! snapshot and an empty store is correct by construction.
//!
//! Shape: per table (object id), a map from *row identity* — the clustered key
//! bytes for a tree table, the stable home RID for a heap — to a chain of
//! versions, newest first. The chain's head describes the current physical
//! state (present, with the bytes living in the table itself; or deleted);
//! older entries carry the row image that was current until the next-newer
//! entry's writer replaced it. Everything is read and written under the one
//! storage mutex, so publication is atomic with the page mutations it
//! describes.
//!
//! Timestamps are *commit sequence numbers* assigned under the storage mutex
//! in commit order. A snapshot is the durable prefix of that order: the last
//! sequence whose commit record the group-commit log-writer has fsynced. A
//! commit that is appended but not yet durable is invisible — its writer has
//! not been acknowledged, so no reader may report state that a crash could
//! take back. Lock-based readers get the same guarantee from release timing
//! (locks release only after the batch's fsync), so versioned reads never see
//! more than locked reads could.

use std::collections::{BTreeMap, HashMap, HashSet};

/// Stamp for row images that predate tracking (the row existed before the
/// first tracked modification): visible to every snapshot.
const ANCIENT_TXN: u64 = 0;

/// A statement's (or, for SNAPSHOT isolation, a transaction's) read view:
/// commits with sequence `<= seq` are visible, plus the session's own open
/// transaction.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadSnapshot {
    pub seq: u64,
    pub own_txn: Option<u64>,
}

/// One staged row change, recorded by the DML paths as each page op succeeds
/// and published to the store when the statement completes (same mutex hold),
/// so readers never see pages and chains disagree.
pub(crate) struct PendingVersion {
    pub object_id: u32,
    pub identity: Vec<u8>,
    pub change: RowChange,
}

pub(crate) enum RowChange {
    Insert,
    Update { prior: Vec<u8> },
    Delete { prior: Vec<u8> },
}

/// How a publish altered its chain — kept in the transaction's publish log so
/// rollback (full or to a savepoint) can reverse the publication exactly.
pub(crate) struct PublishRecord {
    object_id: u32,
    identity: Vec<u8>,
    kind: PublishKind,
}

enum PublishKind {
    /// The chain did not exist; the publish created it (and, for update or
    /// delete, seeded the pre-tracking image below the new head).
    FreshChain,
    /// The head was a present row: it was demoted to an image and the new
    /// state pushed above it.
    OnExisting,
    /// The head was a deleted marker (an insert re-creating the identity):
    /// the new state was pushed above it, nothing was demoted.
    OnDeleted,
}

/// What a snapshot sees for one identity.
#[derive(Debug, PartialEq)]
pub(crate) enum Resolved {
    /// The current physical row is the visible version.
    Current,
    /// This older image is visible instead of the physical state.
    Image(Vec<u8>),
    /// No version is visible (created after the snapshot, or deleted before
    /// it and re-created after).
    Gone,
}

enum EntryState {
    /// Row physically present as of this entry. `image` is `None` only at the
    /// chain head — the bytes are the table's — and `Some` everywhere below.
    Present {
        image: Option<Vec<u8>>,
    },
    Deleted,
}

struct VersionEntry {
    txn: u64,
    state: EntryState,
}

/// Newest first; the head describes the current physical state.
struct Chain {
    entries: Vec<VersionEntry>,
}

#[derive(Default)]
pub(crate) struct VersionState {
    /// Database options (mirrored to the superblock's reserved area).
    pub rcsi: bool,
    pub allow_snapshot: bool,
    /// Committed transactions -> commit sequence. Entries live until no chain
    /// references them and the watermark has passed (pruned together with the
    /// chains). A transaction absent here is running or rolled back — either
    /// way invisible to every snapshot but its own.
    commits: HashMap<u64, u64>,
    /// (commit-record LSN, sequence), ascending in both (assigned under one
    /// mutex in one order) — the durable-prefix search for snapshot capture.
    commit_points: Vec<(u64, u64)>,
    /// The largest sequence pruned out of `commit_points` (its LSN was already
    /// durable), so capture never loses it.
    durable_floor: u64,
    /// Next commit sequence (0 is reserved for [`ANCIENT_TXN`]).
    next_seq: u64,
    /// object id -> identity -> chain.
    tables: HashMap<u32, HashMap<Vec<u8>, Chain>>,
    /// Active snapshots by sequence (multiset): the prune watermark.
    snapshots: BTreeMap<u64, usize>,
}

impl VersionState {
    /// Whether writes must publish versions (any versioned isolation can be
    /// in use).
    pub fn publishing(&self) -> bool {
        self.rcsi || self.allow_snapshot
    }

    /// Applies `ALTER DATABASE` option changes. Turning the last option off
    /// resets the store: the ALTER holds Database X, so no snapshot can be
    /// live and no chain can be needed again.
    pub fn set_options(&mut self, rcsi: Option<bool>, allow_snapshot: Option<bool>) {
        if let Some(on) = rcsi {
            self.rcsi = on;
        }
        if let Some(on) = allow_snapshot {
            self.allow_snapshot = on;
        }
        if !self.publishing() {
            debug_assert!(self.snapshots.is_empty());
            self.tables.clear();
            self.commits.clear();
            self.commit_points.clear();
            self.durable_floor = 0;
        }
    }

    /// The two option bits as persisted in the superblock reserved area.
    pub fn options_byte(&self) -> u8 {
        (self.rcsi as u8) | ((self.allow_snapshot as u8) << 1)
    }

    pub fn set_options_byte(&mut self, byte: u8) {
        self.rcsi = byte & 1 != 0;
        self.allow_snapshot = byte & 2 != 0;
    }

    /// Records a commit, assigning its sequence. Called under the storage
    /// mutex immediately after the commit record is appended.
    pub fn record_commit(&mut self, txn_id: u64, commit_lsn: u64) {
        if !self.publishing() {
            return;
        }
        self.next_seq += 1;
        let seq = self.next_seq;
        self.commits.insert(txn_id, seq);
        self.commit_points.push((commit_lsn, seq));
    }

    /// The snapshot sequence as of `durable_lsn`: the newest commit whose
    /// record the log-writer has already made durable.
    ///
    /// Strictly `<`: the stored LSN is the commit record's START offset,
    /// while durability watermarks count covered bytes (end offsets), and
    /// every flush target lands on an entry boundary — so a record whose
    /// start equals the watermark is entirely NOT durable, and one whose
    /// start is below it is entirely durable (no boundary falls inside a
    /// record). `<=` here counted an unflushed commit as visible, breaking
    /// this module's crash-visibility contract by exactly one record.
    pub fn durable_seq(&self, durable_lsn: u64) -> u64 {
        let idx = self
            .commit_points
            .partition_point(|&(lsn, _)| lsn < durable_lsn);
        if idx == 0 {
            self.durable_floor
        } else {
            self.commit_points[idx - 1].1
        }
    }

    pub fn register_snapshot(&mut self, seq: u64) {
        *self.snapshots.entry(seq).or_insert(0) += 1;
    }

    pub fn release_snapshot(&mut self, seq: u64) {
        if let Some(count) = self.snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                self.snapshots.remove(&seq);
            }
        } else {
            debug_assert!(false, "released a snapshot that was not registered");
        }
    }

    fn visible(&self, txn: u64, snap: ReadSnapshot) -> bool {
        txn == ANCIENT_TXN
            || snap.own_txn == Some(txn)
            || self.commits.get(&txn).is_some_and(|&seq| seq <= snap.seq)
    }

    /// True if any identity of this table has a chain (the read paths' fast
    /// gate: no chains, no merge work).
    pub fn table_has_chains(&self, object_id: u32) -> bool {
        self.tables
            .get(&object_id)
            .is_some_and(|chains| !chains.is_empty())
    }

    /// What `snap` sees for this identity; `None` = no chain (the physical
    /// state is the only version and is visible).
    pub fn resolve(&self, object_id: u32, identity: &[u8], snap: ReadSnapshot) -> Option<Resolved> {
        let chain = self.tables.get(&object_id)?.get(identity)?;
        for (idx, entry) in chain.entries.iter().enumerate() {
            if !self.visible(entry.txn, snap) {
                continue;
            }
            return Some(match &entry.state {
                EntryState::Present { image: None } => {
                    debug_assert_eq!(idx, 0, "imageless entry below the chain head");
                    Resolved::Current
                }
                EntryState::Present { image: Some(image) } => Resolved::Image(image.clone()),
                EntryState::Deleted => Resolved::Gone,
            });
        }
        Some(Resolved::Gone)
    }

    /// Images visible to `snap` for identities a physical read did not
    /// encounter: rows deleted (or re-keyed away, or whose index entry moved)
    /// by writers the snapshot does not see. The caller appends them and lets
    /// its predicate re-check each.
    pub fn unseen_images(
        &self,
        object_id: u32,
        seen: &HashSet<Vec<u8>>,
        snap: ReadSnapshot,
    ) -> Vec<Vec<u8>> {
        let Some(chains) = self.tables.get(&object_id) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for identity in chains.keys() {
            if seen.contains(identity) {
                continue;
            }
            // Only images are appended: `Current` for an unseen identity
            // means the visible version is the physical row itself, and a
            // physical read that did not produce it has already decided it
            // does not qualify (an index seek whose range the row's current
            // value is outside).
            if let Some(Resolved::Image(image)) = self.resolve(object_id, identity, snap) {
                out.push(image);
            }
        }
        out
    }

    /// Publishes one staged change under `txn_id`, returning the record that
    /// lets a rollback reverse it.
    pub fn publish(&mut self, pending: PendingVersion, txn_id: u64) -> PublishRecord {
        let PendingVersion {
            object_id,
            identity,
            change,
        } = pending;
        // An insert has no prior image; updates and deletes carry the bytes
        // that were current until now.
        let (is_delete, prior) = match change {
            RowChange::Insert => (false, None),
            RowChange::Update { prior } => (false, Some(prior)),
            RowChange::Delete { prior } => (true, Some(prior)),
        };
        let new_head = VersionEntry {
            txn: txn_id,
            state: if is_delete {
                EntryState::Deleted
            } else {
                EntryState::Present { image: None }
            },
        };
        let chains = self.tables.entry(object_id).or_default();
        let kind = match (chains.get_mut(&identity), prior) {
            (None, prior) => {
                let mut entries = vec![new_head];
                // The row existed before tracking: seed its pre-tracking
                // image, visible to every snapshot.
                if let Some(prior) = prior {
                    entries.push(VersionEntry {
                        txn: ANCIENT_TXN,
                        state: EntryState::Present { image: Some(prior) },
                    });
                }
                chains.insert(identity.clone(), Chain { entries });
                PublishKind::FreshChain
            }
            (Some(chain), Some(prior)) => {
                // Update/delete: demote the present head to an image, push
                // the new state above it.
                if let Some(head) = chain.entries.first_mut() {
                    debug_assert!(
                        matches!(head.state, EntryState::Present { image: None }),
                        "update/delete over a chain whose head is not the current row"
                    );
                    if let EntryState::Present { image } = &mut head.state {
                        *image = Some(prior);
                    }
                }
                chain.entries.insert(0, new_head);
                PublishKind::OnExisting
            }
            (Some(chain), None) => {
                // Insert re-creating a deleted identity: nothing to demote.
                debug_assert!(
                    matches!(chain.entries.first(), Some(e) if matches!(e.state, EntryState::Deleted)),
                    "insert over a chain whose head is a present row"
                );
                chain.entries.insert(0, new_head);
                PublishKind::OnDeleted
            }
        };
        PublishRecord {
            object_id,
            identity,
            kind,
        }
    }

    /// Reverses one publish (rollback). Must be called in reverse publish
    /// order so nested demotions unwind correctly.
    pub fn unpublish(&mut self, record: PublishRecord, txn_id: u64) {
        let Some(chains) = self.tables.get_mut(&record.object_id) else {
            debug_assert!(false, "unpublish for a table with no chains");
            return;
        };
        match record.kind {
            PublishKind::FreshChain => {
                let removed = chains.remove(&record.identity);
                debug_assert!(
                    removed.is_some_and(|c| c.entries.first().is_some_and(|e| e.txn == txn_id)),
                    "fresh-chain unpublish found someone else's chain"
                );
            }
            PublishKind::OnDeleted | PublishKind::OnExisting => {
                let Some(chain) = chains.get_mut(&record.identity) else {
                    debug_assert!(false, "unpublish for a missing chain");
                    return;
                };
                debug_assert!(chain.entries.first().is_some_and(|e| e.txn == txn_id));
                chain.entries.remove(0);
                if matches!(record.kind, PublishKind::OnExisting) {
                    // The entry below was the head we demoted: the physical
                    // undo restored exactly its image, so it is the current
                    // row again.
                    if let Some(head) = chain.entries.first_mut()
                        && let EntryState::Present { image } = &mut head.state
                    {
                        *image = None;
                    }
                }
            }
        }
        if chains.is_empty() {
            self.tables.remove(&record.object_id);
        }
    }

    /// The oldest sequence any live snapshot needs, or `fallback` (the current
    /// durable sequence) when none is active.
    pub fn watermark(&self, fallback: u64) -> u64 {
        self.snapshots
            .keys()
            .next()
            .copied()
            .unwrap_or(fallback)
            .min(fallback)
    }

    /// Drops chain history no snapshot at or below `watermark` can need,
    /// chains of dropped tables, and commit records nothing references.
    /// `alive` is the set of object ids still in the catalog.
    pub fn prune(&mut self, watermark: u64, alive: &HashSet<u32>) {
        let snap = ReadSnapshot {
            seq: watermark,
            own_txn: None,
        };
        // Collect visibility decisions first: `visible` borrows `self`.
        let mut keep: HashMap<u32, HashMap<Vec<u8>, usize>> = HashMap::new();
        for (&oid, chains) in &self.tables {
            let per_table = keep.entry(oid).or_default();
            for (identity, chain) in chains {
                let newest_visible = chain.entries.iter().position(|e| self.visible(e.txn, snap));
                // Entries below the newest watermark-visible one can never be
                // served again; usize::MAX = keep everything (nothing visible
                // yet: every entry belongs to newer commits or open txns).
                per_table.insert(identity.clone(), newest_visible.unwrap_or(usize::MAX));
            }
        }
        for (oid, chains) in &mut self.tables {
            if !alive.contains(oid) {
                chains.clear();
                continue;
            }
            let per_table = &keep[oid];
            chains.retain(|identity, chain| {
                let cut = per_table[identity];
                if cut == usize::MAX {
                    return true;
                }
                chain.entries.truncate(cut + 1);
                // A chain reduced to its head alone carries no history: the
                // physical state is the only version, which is what "no
                // chain" already means.
                chain.entries.len() > 1
            });
        }
        self.tables.retain(|_, chains| !chains.is_empty());

        // Commits: referenced by a surviving chain, or newer than the
        // watermark (a live or future snapshot may still need the sequence).
        let mut referenced: HashSet<u64> = HashSet::new();
        for chains in self.tables.values() {
            for chain in chains.values() {
                for entry in &chain.entries {
                    referenced.insert(entry.txn);
                }
            }
        }
        self.commits
            .retain(|txn, seq| *seq > watermark || referenced.contains(txn));
        // Commit points whose sequence the watermark has passed are only
        // needed as the durable floor.
        let cut = self
            .commit_points
            .partition_point(|&(_, seq)| seq <= watermark);
        if cut > 0 {
            self.durable_floor = self.durable_floor.max(self.commit_points[cut - 1].1);
            self.commit_points.drain(..cut);
        }
    }

    /// Test observability: chains held for one table.
    #[cfg(test)]
    pub fn chain_count(&self, object_id: u32) -> usize {
        self.tables.get(&object_id).map_or(0, HashMap::len)
    }
}

/// The stable identity of a heap row: its home RID, encoded.
pub(crate) fn rid_identity(rid: crate::relstore::heap::Rid) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    out.extend_from_slice(&rid.page.to_le_bytes());
    out.extend_from_slice(&rid.slot.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const OID: u32 = 7;

    fn on() -> VersionState {
        let mut v = VersionState::default();
        v.set_options(Some(true), None);
        v
    }

    fn snap(v: &VersionState) -> ReadSnapshot {
        ReadSnapshot {
            seq: v.durable_seq(u64::MAX),
            own_txn: None,
        }
    }

    fn pending(identity: &[u8], change: RowChange) -> PendingVersion {
        PendingVersion {
            object_id: OID,
            identity: identity.to_vec(),
            change,
        }
    }

    #[test]
    fn an_update_serves_the_old_image_until_its_commit_is_visible() {
        let mut v = on();
        // Snapshot taken before txn 10 touches the row.
        let before = snap(&v);
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"old".to_vec(),
                },
            ),
            10,
        );
        assert_eq!(
            v.resolve(OID, b"k", before),
            Some(Resolved::Image(b"old".to_vec())),
            "an uncommitted writer's row serves its prior image"
        );
        // The writer's own snapshot sees the current row.
        let own = ReadSnapshot {
            seq: before.seq,
            own_txn: Some(10),
        };
        assert_eq!(v.resolve(OID, b"k", own), Some(Resolved::Current));
        v.record_commit(10, 100);
        assert_eq!(
            v.resolve(OID, b"k", before),
            Some(Resolved::Image(b"old".to_vec())),
            "a snapshot from before the commit still reads the old image"
        );
        let after = snap(&v);
        assert_eq!(v.resolve(OID, b"k", after), Some(Resolved::Current));
    }

    #[test]
    fn a_delete_is_gone_after_commit_and_an_unseen_image_before() {
        let mut v = on();
        let before = snap(&v);
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Delete {
                    prior: b"row".to_vec(),
                },
            ),
            10,
        );
        // The physical row is gone: the scan never encounters `k`, and the
        // merge pass serves its image to the older snapshot.
        let unseen = v.unseen_images(OID, &HashSet::new(), before);
        assert_eq!(unseen, vec![b"row".to_vec()]);
        v.record_commit(10, 100);
        let after = snap(&v);
        assert!(v.unseen_images(OID, &HashSet::new(), after).is_empty());
        assert_eq!(v.resolve(OID, b"k", after), Some(Resolved::Gone));
    }

    #[test]
    fn an_insert_is_invisible_until_commit() {
        let mut v = on();
        let before = snap(&v);
        let _ = v.publish(pending(b"k", RowChange::Insert), 10);
        assert_eq!(
            v.resolve(OID, b"k", before),
            Some(Resolved::Gone),
            "a row inserted by an invisible transaction does not exist yet"
        );
        v.record_commit(10, 100);
        assert_eq!(v.resolve(OID, b"k", snap(&v)), Some(Resolved::Current));
    }

    #[test]
    fn unpublish_reverses_in_reverse_order_including_double_touch() {
        let mut v = on();
        // Txn 10 updates the same row twice, then rolls back.
        let r1 = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"v0".to_vec(),
                },
            ),
            10,
        );
        let r2 = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"v1".to_vec(),
                },
            ),
            10,
        );
        let s = snap(&v);
        assert_eq!(
            v.resolve(OID, b"k", s),
            Some(Resolved::Image(b"v0".to_vec()))
        );
        v.unpublish(r2, 10);
        v.unpublish(r1, 10);
        assert_eq!(
            v.resolve(OID, b"k", s),
            None,
            "a fully unpublished chain is gone (the physical row is the only version)"
        );
        assert_eq!(v.chain_count(OID), 0);
    }

    #[test]
    fn a_key_swap_keeps_both_histories_in_op_order() {
        let mut v = on();
        let before = snap(&v);
        // UPDATE swapping keys A and B: deletes first, inserts after, exactly
        // as the tree applies them.
        let _ = v.publish(
            pending(
                b"a",
                RowChange::Delete {
                    prior: b"rowA".to_vec(),
                },
            ),
            10,
        );
        let _ = v.publish(
            pending(
                b"b",
                RowChange::Delete {
                    prior: b"rowB".to_vec(),
                },
            ),
            10,
        );
        let _ = v.publish(pending(b"b", RowChange::Insert), 10);
        let _ = v.publish(pending(b"a", RowChange::Insert), 10);
        // Both identities are physically present (re-inserted), so a real
        // scan encounters them and nothing is unseen.
        let seen: HashSet<Vec<u8>> = [b"a".to_vec(), b"b".to_vec()].into();
        assert!(v.unseen_images(OID, &seen, before).is_empty());
        assert_eq!(
            v.resolve(OID, b"a", before),
            Some(Resolved::Image(b"rowA".to_vec()))
        );
        assert_eq!(
            v.resolve(OID, b"b", before),
            Some(Resolved::Image(b"rowB".to_vec()))
        );
        v.record_commit(10, 100);
        let after = snap(&v);
        assert_eq!(v.resolve(OID, b"a", after), Some(Resolved::Current));
        assert_eq!(v.resolve(OID, b"b", after), Some(Resolved::Current));
    }

    #[test]
    fn snapshots_are_bounded_by_the_durable_prefix() {
        let mut v = on();
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"old".to_vec(),
                },
            ),
            10,
        );
        v.record_commit(10, 500);
        // The commit record STARTS at LSN 500; watermarks count covered
        // bytes. A watermark at exactly 500 covers nothing of the record —
        // the commit must stay invisible until the watermark passes it.
        let undurable = ReadSnapshot {
            seq: v.durable_seq(500),
            own_txn: None,
        };
        assert_eq!(
            v.resolve(OID, b"k", undurable),
            Some(Resolved::Image(b"old".to_vec()))
        );
        let durable = ReadSnapshot {
            seq: v.durable_seq(501),
            own_txn: None,
        };
        assert_eq!(v.resolve(OID, b"k", durable), Some(Resolved::Current));
    }

    #[test]
    fn prune_respects_the_oldest_live_snapshot_and_drops_after_release() {
        let mut v = on();
        let alive: HashSet<u32> = [OID].into();
        let before = snap(&v);
        v.register_snapshot(before.seq);
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"old".to_vec(),
                },
            ),
            10,
        );
        v.record_commit(10, 100);
        let fallback = v.durable_seq(u64::MAX);
        v.prune(v.watermark(fallback), &alive);
        assert_eq!(
            v.resolve(OID, b"k", before),
            Some(Resolved::Image(b"old".to_vec())),
            "a live snapshot pins the history it may still read"
        );
        v.release_snapshot(before.seq);
        let fallback = v.durable_seq(u64::MAX);
        v.prune(v.watermark(fallback), &alive);
        assert_eq!(v.chain_count(OID), 0, "released history is dropped");
        assert_eq!(v.resolve(OID, b"k", snap(&v)), None);
    }

    #[test]
    fn prune_drops_chains_of_dropped_tables_but_keeps_open_transactions() {
        let mut v = on();
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"old".to_vec(),
                },
            ),
            10,
        );
        // Txn 10 is still open (no commit): its history must survive any
        // prune — a snapshot captured after its commit may need the image.
        let fallback = v.durable_seq(u64::MAX);
        v.prune(v.watermark(fallback), &[OID].into());
        assert_eq!(v.chain_count(OID), 1);
        // The table is dropped: the chain goes regardless.
        v.prune(v.watermark(fallback), &HashSet::new());
        assert_eq!(v.chain_count(OID), 0);
    }

    #[test]
    fn turning_the_last_option_off_resets_the_store() {
        let mut v = on();
        let _ = v.publish(
            pending(
                b"k",
                RowChange::Update {
                    prior: b"old".to_vec(),
                },
            ),
            10,
        );
        v.record_commit(10, 100);
        v.set_options(Some(false), None);
        assert!(!v.publishing());
        assert_eq!(v.chain_count(OID), 0);
        assert_eq!(v.options_byte(), 0);
    }
}
