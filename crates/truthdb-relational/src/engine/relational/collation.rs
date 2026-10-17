//! Collation lives in the SQL crate, so the query layer and the storage layer
//! share one definition of what "equal" and "ordered" mean — index keys and
//! `WHERE =` compare the same sort-key bytes.

pub use truthdb_sql::collation::{Collation, DEFAULT_COLLATION};
