//! Rowsets another engine serves to SQL: built-in table-valued functions such
//! as the object store's `sys.s3_objects(...)`, provided by the code that owns
//! that engine's data (the relational engine does not know its layout). A
//! provider streams its rows slice by slice and takes the storage lock itself
//! for each slice, so it is never called under that lock.

use truthdb_sql::error::SqlError;
use truthdb_sql::value::SqlValue;

use super::api::{ResultColumn, RowSet};
use crate::relstore::types::Datum;

/// Who is asking: a provider whose data is server-wide refuses anyone short of
/// a server administrator.
#[derive(Debug, Clone, Copy)]
pub struct ExternalCaller {
    /// A `sysadmin` login, or the trusted native session.
    pub server_admin: bool,
}

/// The built-in rowsets of another engine.
pub trait ExternalRowsets: Send + Sync {
    /// The rowset `name(args)` (`name` lowercased, schema included), or `None`
    /// when `name` is not one of this provider's.
    fn open(
        &self,
        name: &str,
        args: &[SqlValue],
        caller: ExternalCaller,
    ) -> Option<Result<ExternalRowset, SqlError>>;
}

/// The built-in procedures of another engine (the object store's
/// `sys.sp_s3_*`). One runs outside the caller's transaction, in
/// transactions of its own: a rollback does not undo it. It takes the
/// storage lock itself, so it is never called under that lock.
pub trait ExternalProcedures: Send + Sync {
    /// Whether `name` (lowercased, schema included) is one of this
    /// provider's.
    fn handles(&self, name: &str) -> bool;

    /// Runs procedure `name` with `args`, each named (`@x` as `x`,
    /// lowercased) or positional, in order. Returns its result set, if it
    /// has one.
    fn run(
        &self,
        name: &str,
        args: &[(Option<String>, SqlValue)],
        caller: ExternalCaller,
    ) -> Result<Option<RowSet>, SqlError>;
}

/// An opened rowset: its columns and the stream of its rows.
pub struct ExternalRowset {
    pub columns: Vec<ResultColumn>,
    pub rows: Box<dyn ExternalStream>,
}

/// Rows pulled slice by slice. Each call takes and releases the storage lock
/// itself; it is never made under it.
pub trait ExternalStream: Send {
    fn next_slice(&mut self) -> Result<Option<Vec<Vec<Datum>>>, SqlError>;
}
