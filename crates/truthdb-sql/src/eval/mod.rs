//! Expression evaluation over a bound row of [`SqlValue`](crate::value::SqlValue)s.
//!
//! Column references are resolved to indices before evaluation (by the binder
//! in the storage crate), so [`eval`] takes the row as a slice and a resolver
//! mapping a column [`Name`](crate::ast::Name) to its index. Arithmetic and
//! comparisons follow three-valued logic.

mod arithmetic;
mod casts;
mod comparison;
mod context;
mod expression;
mod functions;

pub use arithmetic::arith;
pub use comparison::key_collation;
pub use context::{
    ColumnResolver, DEFAULT_DATABASE_ID, ErrorInfo, EvalContext, Resolution, SecurityContext,
    UpdatedColumns,
};
pub use expression::eval;

#[cfg(test)]
mod tests;
