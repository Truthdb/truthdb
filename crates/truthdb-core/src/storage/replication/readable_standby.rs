use super::super::*;

impl StorageFile {
    /// Maintains the standby's active-transaction table as shipped records are
    /// applied — the SAME begin/resolve rules recovery's analysis uses
    /// (`analyze_and_redo`): TXN_BEGIN opens at its own LSN, TXN_COMMIT and
    /// TXN_END resolve. A page op for a transaction whose BEGIN was not seen
    /// cannot arise (a chunk never precedes a transaction's BEGIN — chunks
    /// apply in order and BEGIN is its first record); the defensive
    /// `or_insert` still clamps the floor at that record if it ever did.
    pub(in crate::storage) fn standby_track_rel_record(&mut self, lsn: u64, record: &RelRecord) {
        use crate::wal::records::{REL_KIND_TXN_BEGIN, REL_KIND_TXN_COMMIT, REL_KIND_TXN_END};
        match record.kind {
            REL_KIND_TXN_BEGIN => {
                self.standby_att.insert(record.txn_id, lsn);
            }
            REL_KIND_TXN_COMMIT | REL_KIND_TXN_END => {
                self.standby_att.remove(&record.txn_id);
            }
            _ => {
                if record.txn_id != 0 {
                    self.standby_att.entry(record.txn_id).or_insert(lsn);
                }
            }
        }
    }
}
