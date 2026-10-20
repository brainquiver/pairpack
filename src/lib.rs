//! Store a string column of a parquet shard as character edits of another string column of the same row, and
//! rebuild it byte for byte.
//!
//! Parquet compresses each column on its own, and zstd cannot see that two columns hold near copies of one passage.
//! The edits keep one copy and the changes. A row keeps its derived string in full where the edits would cost more.

pub mod diff;
pub mod edits;
pub mod shard;
pub mod token;

pub use edits::{apply, choose, edits_of, Edit, Stored};
