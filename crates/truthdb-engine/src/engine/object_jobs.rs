//! The object engine's background work beyond the maintenance sweep: packing
//! bodies of cold storage classes (see `truthdb_object::PackStep`), a segment
//! per step, and the buckets' daily lifecycle passes (see
//! `truthdb_object::LifecycleConfig`), a bounded batch per step. A packing
//! step reads its segment under the storage lock, compresses it without the
//! lock, and applies the frame in a transaction of its own. A standby does
//! none of it (its log is the primary's).

use truthdb_object::{PackStep, StepBudget};

use super::{Engine, EngineError};

/// What one lifecycle step may do under one storage-lock hold.
const LIFECYCLE_STEP: StepBudget = StepBudget {
    rows: 512,
    actions: 16,
};

/// How long a body that a lifecycle expiration dropped outlives it: readers
/// that resolved the version just before may still be reading.
const LIFECYCLE_DROP_GRACE_MS: u64 = 15 * 60 * 1000;

impl Engine {
    /// One step of the object jobs; returns whether work remains.
    pub(crate) fn object_jobs_step(&self) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
        let packed = self.pack_step()?;
        let now_ms = super::now_ms();
        let lifecycle = self
            .object(|file| file.lifecycle_step(now_ms, LIFECYCLE_DROP_GRACE_MS, LIFECYCLE_STEP))?;
        Ok(packed || lifecycle)
    }

    /// One packing step; returns whether there was one.
    fn pack_step(&self) -> Result<bool, EngineError> {
        match self.object(|file| file.next_pack())? {
            None => Ok(false),
            Some(PackStep::Finish { body, level }) => {
                self.object(|file| file.transaction(|tx| tx.finish_pack(body, level)))?;
                Ok(true)
            }
            Some(PackStep::Unreadable {
                body,
                level,
                index,
                error,
            }) => {
                // Left as it is (its readers see the damage too); the bodies
                // queued after it go on.
                eprintln!("object jobs: body {body} cannot be packed: {error}");
                self.object(|file| file.transaction(|tx| tx.drop_pack(body, level, index)))?;
                Ok(true)
            }
            Some(PackStep::Segment(work)) => {
                let frame = work.compress();
                self.object(|file| file.transaction(|tx| tx.apply_pack(&work, frame)))?;
                Ok(true)
            }
        }
    }
}
