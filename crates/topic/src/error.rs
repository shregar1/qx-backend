use thiserror::Error;

#[derive(Debug, Error)]
pub enum TopicError {
    #[error("queue: {0}")]
    Queue(#[from] qx_queue::QueueError),

    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("partition count mismatch: configured {configured}, on-disk {on_disk}")]
    PartitionCountMismatch { configured: u32, on_disk: u32 },

    #[error("corrupt topic metadata")]
    CorruptMetadata,

    #[error("invalid partition: {partition} (topic has {count})")]
    InvalidPartition { partition: u32, count: u32 },

    #[error("partition_count must be at least 1")]
    ZeroPartitions,

    /// Catch-all for errors that don't fit the strongly-typed variants
    /// — primarily used by the replication seam to surface failures
    /// from Raft (serialization, leader timeout, channel closed) up to
    /// the broker's Produce handler.
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, TopicError>;
