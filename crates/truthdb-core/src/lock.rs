//! Database/table/row lock manager (Stage 6, row locks added in Stage 12):
//! two-phase locking that serializes conflicting transactions across sessions.
//!
//! Worker threads never block in place on a lock — the
//! [`session`](crate::session) scheduler acquires a batch's locks up front and
//! *parks* the whole request when one conflicts (see that module). This type is
//! the pure bookkeeping underneath: which owner holds what, and whether a
//! requested mode conflicts. Point INSERT/UPDATE/DELETE take a [`Resource::Row`]
//! lock so writers to different rows of one table need not serialize; scans and
//! non-point predicates stay table-granular.

use std::collections::HashMap;

/// A lockable resource, forming a `Database` → `Table` → `Row` hierarchy.
/// `Database` is the intent-lock root; `Table` is keyed by catalog object id;
/// `Row` is keyed by `(table object id, key hash)` — the xxh64 of the row's
/// clustered-key bytes (Stage 12). A row lock keeps [`Resource`] `Copy` by
/// hashing the key rather than carrying its bytes; a hash collision only ever
/// makes two unrelated rows share a lock queue (a rare, harmless over-serialize)
/// — it can never let two distinct keys skip a real conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resource {
    Database,
    Table(u32),
    Row(u32, u64),
}

/// Lock modes, weakest to strongest by [`LockMode::rank`]. Intent modes
/// (`IntentShared`/`IntentExclusive`) sit on the `Database` parent; base
/// modes (`Shared`/`Exclusive`) sit on tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    IntentShared,
    IntentExclusive,
    Shared,
    Exclusive,
}

impl LockMode {
    /// Rank in the lock lattice, for [`LockMode::combine`].
    fn rank(self) -> u8 {
        match self {
            LockMode::IntentShared => 0,
            LockMode::IntentExclusive => 1,
            LockMode::Shared => 2,
            LockMode::Exclusive => 3,
        }
    }

    /// The least mode an owner needs to cover both `self` and `other` on one
    /// resource (the lattice join). `S` and `IX` are incomparable — their true
    /// join is `SIX`, which this engine has no mode for, so it uses the stronger
    /// `X`. This case is real since Stage 12: a batch that point-writes a row
    /// (Table `IX`) *and* reads the same table (Table `S`) must end up with an
    /// exclusive table lock, not `S` — otherwise max-by-rank would silently drop
    /// the write intent and expose the uncommitted row to table-level readers.
    pub fn combine(self, other: LockMode) -> LockMode {
        use LockMode::*;
        match (self, other) {
            (Shared, IntentExclusive) | (IntentExclusive, Shared) => Exclusive,
            _ if self.rank() >= other.rank() => self,
            _ => other,
        }
    }

    /// A read mode (`S`/`IS`) — released per-statement under READ COMMITTED,
    /// held to commit under REPEATABLE READ / SERIALIZABLE.
    fn is_read(self) -> bool {
        matches!(self, LockMode::IntentShared | LockMode::Shared)
    }

    /// Whether two modes held on the same resource by *different* owners can
    /// coexist. Standard IS/IX/S/X compatibility matrix; symmetric.
    fn compatible_with(self, other: LockMode) -> bool {
        use LockMode::*;
        !matches!(
            (self, other),
            (IntentShared, Exclusive)
                | (Exclusive, IntentShared)
                | (IntentExclusive, Shared)
                | (Shared, IntentExclusive)
                | (IntentExclusive, Exclusive)
                | (Exclusive, IntentExclusive)
                | (Shared, Exclusive)
                | (Exclusive, Shared)
                | (Exclusive, Exclusive)
        )
    }
}

/// One held lock: an owner and the (possibly upgraded) mode.
#[derive(Debug, Clone, Copy)]
struct Grant {
    owner: u64,
    mode: LockMode,
}

/// Tracks granted locks per resource. Owners are session ids. This structure
/// carries no wait queue — parking and FIFO fairness live in the engine loop,
/// which owns the parked requests and their reply channels.
#[derive(Default)]
pub struct LockManager {
    grants: HashMap<Resource, Vec<Grant>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager::default()
    }

    /// The owner already blocking `owner` from taking `mode` on `resource`, if
    /// any (a single conflicting owner suffices to block). An owner never
    /// conflicts with its own held lock (upgrades are allowed).
    pub fn conflict(&self, owner: u64, resource: Resource, mode: LockMode) -> Option<u64> {
        let grants = self.grants.get(&resource)?;
        grants
            .iter()
            .find(|g| g.owner != owner && !g.mode.compatible_with(mode))
            .map(|g| g.owner)
    }

    /// Every other owner whose held lock on `resource` conflicts with `mode` —
    /// i.e. all the owners a would-be acquirer must wait for. Used to build the
    /// waits-for graph for deadlock detection.
    pub fn conflicting_holders(&self, owner: u64, resource: Resource, mode: LockMode) -> Vec<u64> {
        let Some(grants) = self.grants.get(&resource) else {
            return Vec::new();
        };
        grants
            .iter()
            .filter(|g| g.owner != owner && !g.mode.compatible_with(mode))
            .map(|g| g.owner)
            .collect()
    }

    /// Whether `owner` already holds any lock on `resource`. Used to exempt a
    /// re-acquisition or upgrade from FIFO anti-barging: an owner that already
    /// holds a resource is not jumping the queue for it.
    pub fn holds(&self, owner: u64, resource: Resource) -> bool {
        self.grants
            .get(&resource)
            .is_some_and(|grants| grants.iter().any(|g| g.owner == owner))
    }

    /// Grants (or upgrades) `owner`'s lock on `resource` to at least `mode`.
    /// The caller must have checked [`LockManager::conflict`] first.
    pub fn grant(&mut self, owner: u64, resource: Resource, mode: LockMode) {
        let grants = self.grants.entry(resource).or_default();
        if let Some(existing) = grants.iter_mut().find(|g| g.owner == owner) {
            existing.mode = existing.mode.combine(mode);
        } else {
            grants.push(Grant { owner, mode });
        }
    }

    /// Releases every lock held by `owner` (transaction end / disconnect).
    /// Returns the resources that had a grant removed, so the caller can wake
    /// waiters queued on them.
    pub fn release_all(&mut self, owner: u64) -> Vec<Resource> {
        let mut freed = Vec::new();
        self.grants.retain(|resource, grants| {
            let before = grants.len();
            grants.retain(|g| g.owner != owner);
            if grants.len() != before {
                freed.push(*resource);
            }
            !grants.is_empty()
        });
        freed
    }

    /// Releases only `owner`'s read locks (`S`/`IS`), keeping write locks.
    /// Used at statement end under READ COMMITTED, where shared locks do not
    /// survive the statement. Returns the resources that were freed.
    pub fn release_read_locks(&mut self, owner: u64) -> Vec<Resource> {
        let mut freed = Vec::new();
        self.grants.retain(|resource, grants| {
            let before = grants.len();
            grants.retain(|g| !(g.owner == owner && g.mode.is_read()));
            if grants.len() != before {
                freed.push(*resource);
            }
            !grants.is_empty()
        });
        freed
    }

    /// True if `owner` holds no locks at all.
    #[cfg(test)]
    fn holds_nothing(&self, owner: u64) -> bool {
        self.grants
            .values()
            .all(|grants| grants.iter().all(|g| g.owner != owner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: u64 = 1;
    const B: u64 = 2;
    const T: Resource = Resource::Table(10);

    #[test]
    fn compatibility_matrix_is_standard() {
        use LockMode::*;
        // Shared readers coexist; a writer excludes everyone.
        assert!(Shared.compatible_with(Shared));
        assert!(!Shared.compatible_with(Exclusive));
        assert!(!Exclusive.compatible_with(Shared));
        assert!(!Exclusive.compatible_with(Exclusive));
        // Intent locks coexist with each other.
        assert!(IntentShared.compatible_with(IntentExclusive));
        assert!(IntentExclusive.compatible_with(IntentExclusive));
        // IX excludes S (a writer's intent blocks a table-wide reader).
        assert!(!IntentExclusive.compatible_with(Shared));
        // IS coexists with S but not X.
        assert!(IntentShared.compatible_with(Shared));
        assert!(!IntentShared.compatible_with(Exclusive));
    }

    #[test]
    fn writer_blocks_reader_across_owners() {
        let mut lm = LockManager::new();
        assert!(lm.conflict(A, T, LockMode::Exclusive).is_none());
        lm.grant(A, T, LockMode::Exclusive);
        // B cannot read while A writes.
        assert_eq!(lm.conflict(B, T, LockMode::Shared), Some(A));
        // A itself is never blocked by its own lock.
        assert!(lm.conflict(A, T, LockMode::Shared).is_none());
    }

    #[test]
    fn shared_readers_share_but_block_a_writer() {
        let mut lm = LockManager::new();
        lm.grant(A, T, LockMode::Shared);
        assert!(lm.conflict(B, T, LockMode::Shared).is_none());
        lm.grant(B, T, LockMode::Shared);
        // A writer must wait for either reader.
        assert!(lm.conflict(A, T, LockMode::Exclusive).is_some());
    }

    #[test]
    fn upgrade_shared_to_exclusive_when_sole_holder() {
        let mut lm = LockManager::new();
        lm.grant(A, T, LockMode::Shared);
        // No other holder → A upgrades to X.
        assert!(lm.conflict(A, T, LockMode::Exclusive).is_none());
        lm.grant(A, T, LockMode::Exclusive);
        // Now B is excluded.
        assert_eq!(lm.conflict(B, T, LockMode::Shared), Some(A));
    }

    #[test]
    fn combine_shared_and_intent_exclusive_escalates_to_exclusive() {
        use LockMode::*;
        // A batch that point-writes a row (Table IX) and reads the same table
        // (Table S) must combine to X — not S, which would drop the write intent.
        assert_eq!(Shared.combine(IntentExclusive), Exclusive);
        assert_eq!(IntentExclusive.combine(Shared), Exclusive);
        // The other pairs are plain max-by-rank.
        assert_eq!(IntentShared.combine(IntentExclusive), IntentExclusive);
        assert_eq!(IntentShared.combine(Shared), Shared);
        assert_eq!(Shared.combine(Exclusive), Exclusive);
        assert_eq!(IntentExclusive.combine(Exclusive), Exclusive);
    }

    #[test]
    fn row_locks_on_distinct_keys_do_not_conflict() {
        let mut lm = LockManager::new();
        let r5 = Resource::Row(10, 5);
        let r6 = Resource::Row(10, 6);
        // Both writers take IX on the table (compatible) + X on their own row.
        lm.grant(A, Resource::Table(10), LockMode::IntentExclusive);
        lm.grant(A, r5, LockMode::Exclusive);
        // B's IX on the table coexists with A's IX.
        assert!(
            lm.conflict(B, Resource::Table(10), LockMode::IntentExclusive)
                .is_none()
        );
        lm.grant(B, Resource::Table(10), LockMode::IntentExclusive);
        // B's X on a different row does not conflict with A's.
        assert!(lm.conflict(B, r6, LockMode::Exclusive).is_none());
        // But B's X on the *same* row does.
        assert_eq!(lm.conflict(B, r5, LockMode::Exclusive), Some(A));
    }

    #[test]
    fn release_all_frees_resources_and_reports_them() {
        let mut lm = LockManager::new();
        lm.grant(A, Resource::Database, LockMode::IntentExclusive);
        lm.grant(A, T, LockMode::Exclusive);
        let freed = lm.release_all(A);
        assert!(freed.contains(&Resource::Database));
        assert!(freed.contains(&T));
        assert!(lm.holds_nothing(A));
        // B can now acquire.
        assert!(lm.conflict(B, T, LockMode::Exclusive).is_none());
    }
}
