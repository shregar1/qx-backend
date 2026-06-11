//! Public types used by the queue API. Implementation details of how
//! state is stored on disk live in [`crate::ranges`].

use qx_storage::RecordId;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending = 0,
    InFlight = 1,
    Dead = 2,
}

/// A handle returned by `receive` and required to ack / nack / extend
/// the visibility of a delivered message. The `attempt` field guards
/// against a stale consumer acking a message that has already timed
/// out and been re-delivered (a different attempt count).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReceiptHandle {
    pub record_id: RecordId,
    pub attempt: u32,
}
