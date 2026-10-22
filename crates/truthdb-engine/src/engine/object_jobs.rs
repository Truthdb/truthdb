//! The object engine's background work beyond the maintenance sweep: packing
//! bodies of cold storage classes (see `truthdb_object::PackStep`), a segment
//! per step. A step reads its segment under the storage lock, compresses it
//! without the lock, and applies the frame in a transaction of its own. A
//! standby does none of it (its log is the primary's).

use truthdb_object::PackStep;

use super::{Engine, EngineError};

impl Engine {
    /// One step of the object jobs; returns whether work remains.
    pub(crate) fn object_jobs_step(&self) -> Result<bool, EngineError> {
        if self.storage.is_standby() || !self.runs_objects() {
            return Ok(false);
        }
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
