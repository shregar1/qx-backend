//! qx-storage — durable append-only log primitives.
//!
//! Layering:
//!   `Segment`         — one append-only file, byte-offset addressed
//!   `IndexedSegment`  — record-id (integer) addressing over a `Segment`
//!   `PartitionLog`    — many `IndexedSegment`s with rolling + recovery

mod error;
mod frame;
mod indexed_segment;
mod log_reader;
mod partition_log;
mod segment;

pub use error::{Result, StorageError};
pub use indexed_segment::{IndexedSegment, RecordId};
pub use log_reader::LogReader;
pub use partition_log::{DEFAULT_MAX_SEGMENT_BYTES, PartitionConfig, PartitionLog};
pub use segment::{Offset, Record, Segment};
