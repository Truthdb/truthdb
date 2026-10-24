//! The API activity log: each account's management events (as CloudTrail
//! records them, a JSON document each), kept 90 days. Events are keyed by
//! account and, newest first, by time, so an account's latest events read
//! from the front; an index by time expires them, a bounded batch at a
//! time.

use truthdb_core::relstore::btree::TREE_MAX_CELL;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::codec::corrupt;
use crate::{ACTIVITY_EXPIRY_TREE, ACTIVITY_TREE, ObjFile, ObjTxn};

/// How long events are kept (ms).
pub const ACTIVITY_KEEP_MS: u64 = 90 * 86_400_000;
/// The largest event document kept (with its key, it fits a tree row).
pub const MAX_ACTIVITY_RECORD: usize = TREE_MAX_CELL - 128;

/// An event's key: the account, then the time newest first, then its id.
fn event_key(account: &str, time_ms: u64, id: &[u8; 8]) -> Vec<u8> {
    [
        account.as_bytes(),
        &[0],
        &(u64::MAX - time_ms).to_be_bytes(),
        id,
    ]
    .concat()
}

/// An expiry index entry: the time, then the event's key.
fn expiry_key(time_ms: u64, event: &[u8]) -> Vec<u8> {
    [&time_ms.to_be_bytes()[..], event].concat()
}

/// One event read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityEvent {
    pub account: String,
    pub time_ms: u64,
    pub id: [u8; 8],
    /// The CloudTrail record (JSON).
    pub record: Vec<u8>,
}

impl ActivityEvent {
    fn from_row(key: &[u8], record: Vec<u8>) -> Result<ActivityEvent, StorageError> {
        let split = key
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| corrupt("activity event"))?;
        let rest = &key[split + 1..];
        if rest.len() != 16 {
            return Err(corrupt("activity event"));
        }
        let account = std::str::from_utf8(&key[..split])
            .map_err(|_| corrupt("activity event"))?
            .to_string();
        let inverted = u64::from_be_bytes(rest[..8].try_into().expect("8 bytes"));
        Ok(ActivityEvent {
            account,
            time_ms: u64::MAX - inverted,
            id: rest[8..].try_into().expect("8 bytes"),
            record,
        })
    }

    /// The key that resumes a listing after this event.
    pub fn resume_key(&self) -> Vec<u8> {
        event_key(&self.account, self.time_ms, &self.id)
    }
}

impl ObjTxn<'_, '_> {
    /// Keeps an event of `account` at `time_ms` (refused if `record` is
    /// larger than `MAX_ACTIVITY_RECORD`; an id already kept is left as it
    /// is).
    pub fn add_activity(
        &mut self,
        account: &str,
        time_ms: u64,
        id: &[u8; 8],
        record: &[u8],
    ) -> Result<(), StorageError> {
        if record.len() > MAX_ACTIVITY_RECORD || account.is_empty() || account.contains('\0') {
            return Err(StorageError::InvalidConfig(
                "an activity event is too large, or names no account".to_string(),
            ));
        }
        let (events, expiry) = (self.trees.activity(), self.trees.activity_expiry());
        let key = event_key(account, time_ms, id);
        events.insert_unique(self.ctx, &mut OpMode::Txn(self.txn), &key, record)?;
        expiry.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &expiry_key(time_ms, &key),
            &[],
        )?;
        Ok(())
    }

    /// Removes up to `limit` events older than `ACTIVITY_KEEP_MS` at
    /// `now_ms`; returns how many went.
    pub fn expire_activity(&mut self, now_ms: u64, limit: usize) -> Result<usize, StorageError> {
        let (events, expiry) = (self.trees.activity(), self.trees.activity_expiry());
        let before = now_ms.saturating_sub(ACTIVITY_KEEP_MS);
        let due = expiry.scan_range_limit(self.ctx, None, Some(&before.to_be_bytes()), limit)?;
        let removed = due.len();
        for (key, _) in due {
            let event = key.get(8..).ok_or_else(|| corrupt("activity expiry"))?;
            events.delete(self.ctx, &mut OpMode::Txn(self.txn), event)?;
            expiry.delete(self.ctx, &mut OpMode::Txn(self.txn), &key)?;
        }
        Ok(removed)
    }
}

impl ObjFile<'_> {
    /// Up to `limit` of `account`'s events (every account's if none), newest
    /// first within each account, after `after` (a `resume_key`).
    pub fn activity(
        &mut self,
        account: Option<&str>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<ActivityEvent>, StorageError> {
        let Some(events) = self.existing_tree(ACTIVITY_TREE)? else {
            return Ok(Vec::new());
        };
        let (lower, upper) = match account {
            Some(account) => (
                Some([account.as_bytes(), &[0]].concat()),
                Some([account.as_bytes(), &[1]].concat()),
            ),
            None => (None, None),
        };
        // Past `after` (a key itself, so one more byte is the next key up).
        let lower = match after {
            Some(after) => Some([after, &[0]].concat()),
            None => lower,
        };
        let mut ctx = self.core.page_ctx(0);
        let rows = events.scan_range_limit(&mut ctx, lower.as_deref(), upper.as_deref(), limit)?;
        rows.into_iter()
            .map(|(key, record)| ActivityEvent::from_row(&key, record))
            .collect()
    }

    /// Whether any event is past its keep at `now_ms`: a sweep with nothing
    /// to expire writes nothing.
    pub fn activity_due(&mut self, now_ms: u64) -> Result<bool, StorageError> {
        let Some(expiry) = self.existing_tree(ACTIVITY_EXPIRY_TREE)? else {
            return Ok(false);
        };
        let before = now_ms.saturating_sub(ACTIVITY_KEEP_MS);
        let mut ctx = self.core.page_ctx(0);
        Ok(!expiry
            .scan_range_limit(&mut ctx, None, Some(&before.to_be_bytes()), 1)?
            .is_empty())
    }
}
