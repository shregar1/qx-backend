//! OffsetForLeaderEpoch (api_key = 23), v4.
//!
//! Fetchers send this to verify they haven't been diverged by a leader
//! change. In single-broker qx we always have `leader_epoch = 0`, so
//! any request that names a past epoch can be answered with our current
//! high watermark — the fetcher's view of "what came after that epoch"
//! is just everything we know about.
//!
//! Request layout (v4, flexible):
//!
//! ```text
//! topics compact_array<{
//!   topic compact_string
//!   partitions compact_array<{
//!     partition           i32
//!     current_leader_epoch i32
//!     leader_epoch         i32
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! tagged_fields
//! ```
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! topics compact_array<{
//!   topic compact_string
//!   partitions compact_array<{
//!     error_code     i16
//!     partition      i32
//!     leader_epoch   i32
//!     end_offset     i64
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 23;
pub const MIN_VERSION: i16 = 4;
pub const MAX_VERSION: i16 = 4;

#[derive(Debug)]
pub struct OffsetForLeaderEpochRequest {
    pub topics: Vec<TopicEpochRequest>,
}

#[derive(Debug)]
pub struct TopicEpochRequest {
    pub topic: String,
    pub partitions: Vec<PartitionEpochRequest>,
}

#[derive(Debug)]
pub struct PartitionEpochRequest {
    pub partition: i32,
    #[allow(dead_code)] // single-broker has no leader-epoch tracking
    pub current_leader_epoch: i32,
    #[allow(dead_code)]
    pub leader_epoch: i32,
}

impl OffsetForLeaderEpochRequest {
    pub fn decode(_version: i16, buf: &[u8]) -> WireResult<Self> {
        let mut r = Reader::new(buf);
        // v4 starts with a leading "replica_id" i32? Actually no — v4 is
        // the version flexible'd, and the body still leads with topics.
        // (librdkafka sends nothing else before the topics array.)
        let n = r.read_uvarint()?;
        let tcount = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(tcount as usize);
        for _ in 0..tcount {
            let topic = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let pcount = if pn == 0 { 0 } else { pn - 1 };
            let mut partitions = Vec::with_capacity(pcount as usize);
            for _ in 0..pcount {
                let partition = r.read_i32()?;
                let current_leader_epoch = r.read_i32()?;
                let leader_epoch = r.read_i32()?;
                r.read_tagged_fields()?;
                partitions.push(PartitionEpochRequest {
                    partition,
                    current_leader_epoch,
                    leader_epoch,
                });
            }
            r.read_tagged_fields()?;
            topics.push(TopicEpochRequest { topic, partitions });
        }
        r.read_tagged_fields()?;
        Ok(Self { topics })
    }
}

pub fn handle_and_encode(w: &mut Writer, broker: &Broker, req: OffsetForLeaderEpochRequest) {
    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(req.topics.len() as u32);
    for t in req.topics {
        w.write_compact_string(&t.topic);
        w.write_compact_array_len(t.partitions.len() as u32);

        let topic_handle = broker.lookup(&t.topic);
        for p in t.partitions {
            let (error_code, end_offset): (i16, i64) = match &topic_handle {
                None => (error_codes::UNKNOWN_TOPIC_OR_PARTITION, -1),
                Some(topic) => {
                    if p.partition < 0 || (p.partition as u32) >= topic.partition_count() {
                        (error_codes::UNKNOWN_TOPIC_OR_PARTITION, -1)
                    } else {
                        match topic.partition_end_offset(p.partition as u32) {
                            Ok(end) => (error_codes::NONE, end as i64),
                            Err(_) => (error_codes::UNKNOWN_SERVER_ERROR, -1),
                        }
                    }
                }
            };
            w.write_i16(error_code);
            w.write_i32(p.partition);
            w.write_i32(0); // leader_epoch — single broker, always 0
            w.write_i64(end_offset);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
}
