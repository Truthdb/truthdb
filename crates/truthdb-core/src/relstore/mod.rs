//! Page-structured storage shared by the engines: the page format, the
//! buffer pool, slotted pages, the B+ tree, the page-op context that logs and
//! applies physiological WAL records, and the ARIES restart driver.

pub mod btree;
pub mod buffer_pool;
pub mod ctx;
pub mod page;
pub mod recovery;
pub mod slotted;
