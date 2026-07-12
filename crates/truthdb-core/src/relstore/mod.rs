//! Relational on-disk structures (Stage 1: page format and buffer pool).
//!
//! Slotted pages, B+ trees and heaps arrive in Stage 2; this module currently
//! provides the shared page header/checksum format and the buffer pool that
//! all relational structures will sit on.

pub mod buffer_pool;
pub mod page;
