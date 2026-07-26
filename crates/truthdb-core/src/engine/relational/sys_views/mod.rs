use super::prelude::*;

mod catalog;
mod configuration;
mod replication;
mod routines;
mod security;

pub(in crate::engine::relational) use catalog::*;
pub(in crate::engine::relational) use configuration::*;
pub(in crate::engine::relational) use replication::*;
pub(in crate::engine::relational) use routines::*;
pub(in crate::engine::relational) use security::*;

// ---- sys.* virtual sources ---------------------------------------------

pub(super) fn nvarchar(name: &str, max_len: u16) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::NVarChar { max_len },
    }
}

pub(super) fn int_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::Int,
    }
}

pub(super) fn bigint_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::BigInt,
    }
}

pub(super) fn bit_col(name: &str) -> ResultColumn {
    ResultColumn {
        name: name.to_string(),
        column_type: ColumnType::Bit,
    }
}
