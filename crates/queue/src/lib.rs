//! qx-queue — queue semantics over an append-only log.
//!
//! Layering:
//!   `qx-storage::PartitionLog`   durable record log
//!   `Queue`                          adds per-message state (Pending/InFlight/Dead),
//!                                    visibility timeouts, ack/nack, DLQ
//!
//! State lives in an embedded `redb` database keyed by record id; the log is
//! the source of truth for payloads.

mod error;
mod queue;
mod state;

pub use error::{QueueError, Result};
pub use queue::{
    DEFAULT_MAX_ATTEMPTS, DEFAULT_VISIBILITY_TIMEOUT, Message, Queue, QueueConfig, QueueStats,
    StreamedMessage,
};
pub use state::{ReceiptHandle, Status};
