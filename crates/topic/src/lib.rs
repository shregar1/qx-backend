//! qx-topic — multi-partition topic abstraction.
//!
//! A `Topic` owns N `Queue` partitions. Producers send with a routing key
//! (deterministic hash → partition) or directly to a partition number;
//! consumers receive across all partitions and acknowledge by `TopicReceipt`,
//! which carries the partition the message came from.

mod cold_store;
mod error;
mod topic;

pub use cold_store::{ColdStore, LocalFsColdStore};
pub use error::{Result, TopicError};
pub use topic::{
    read_partition_count, Topic, TopicConfig, TopicMessage, TopicReceipt, TopicStats,
    TopicStreamedMessage,
};
