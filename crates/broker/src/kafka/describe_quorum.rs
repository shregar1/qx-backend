//! DescribeQuorum (api_key = 55), v1.
//!
//! KRaft-mode admin tools (kafka-metadata-quorum.sh, AdminClient.describeQuorum)
//! hit this to inspect the controller-quorum state. In single-broker
//! qx we synthesize a one-node quorum where this broker is both leader
//! and the only voter. When `BrokerConfig::cluster` is `Some` we
//! advertise the configured voter set.
//!
//! Request layout (v1, flexible):
//!
//! ```text
//! topics compact_array<{
//!   name compact_string
//!   partitions compact_array<{ index i32, tagged_fields }>
//!   tagged_fields
//! }>
//! tagged_fields
//! ```
//!
//! For KRaft the "topic" is always `__cluster_metadata` partition 0; we
//! still decode the request body to consume the bytes cleanly and echo
//! the requested shape in the response.
//!
//! Response:
//! ```text
//! throttle_time_ms i32
//! error_code i16
//! topics compact_array<{
//!   name compact_string
//!   partitions compact_array<{
//!     index i32
//!     error_code i16
//!     leader_id i32
//!     leader_epoch i32
//!     high_watermark i64
//!     current_voters compact_array<{
//!       replica_id i32
//!       log_end_offset i64
//!       last_fetch_timestamp i64
//!       last_caught_up_timestamp i64
//!       tagged_fields
//!     }>
//!     observers compact_array<...>          // same shape as voters
//!     tagged_fields
//!   }>
//!   tagged_fields
//! }>
//! tagged_fields
//! ```

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

pub const API_KEY: i16 = 55;
pub const MIN_VERSION: i16 = 1;
pub const MAX_VERSION: i16 = 1;

pub fn handle_and_encode(
    w: &mut Writer,
    broker: &Broker,
    config: &super::server::KafkaConfig,
    body: &[u8],
) -> WireResult<()> {
    // Decode + walk past the request body.
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    let mut requested: Vec<(String, Vec<i32>)> = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            let idx = r.read_i32()?;
            r.read_tagged_fields()?;
            parts.push(idx);
        }
        r.read_tagged_fields()?;
        requested.push((name, parts));
    }
    r.read_tagged_fields()?;

    // Default to one quorum entry for the KRaft control plane if the
    // client asked for nothing in particular.
    let topics_to_report: Vec<(String, Vec<i32>)> = if requested.is_empty() {
        vec![("__cluster_metadata".to_string(), vec![0])]
    } else {
        requested
    };

    let voters = broker
        .config_cluster_voters()
        .unwrap_or_else(|| vec![config.node_id]);

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_array_len(topics_to_report.len() as u32);
    for (name, parts) in topics_to_report {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for pidx in parts {
            w.write_i32(pidx);
            w.write_i16(error_codes::NONE);
            w.write_i32(config.node_id);     // leader_id — always us in single-broker
            w.write_i32(0);                  // leader_epoch
            w.write_i64(0);                  // high_watermark — control plane not tracked
            w.write_compact_array_len(voters.len() as u32);
            for v in &voters {
                w.write_i32(*v);
                w.write_i64(0);  // log_end_offset
                w.write_i64(-1); // last_fetch_timestamp
                w.write_i64(-1); // last_caught_up_timestamp
                w.write_empty_tagged_fields();
            }
            w.write_compact_array_len(0); // observers
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}
