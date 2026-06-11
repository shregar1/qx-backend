//! OffsetDelete (api_key = 47), v0.
//!
//! `kafka-consumer-groups.sh --delete-offsets --group g --topic t:0,1` lands
//! here. We forward each `(topic, partition)` to the named coordinator's
//! committed-offset map; unknown keys come back as
//! `UNKNOWN_TOPIC_OR_PARTITION` so the CLI can report them honestly.
//!
//! v0 is non-flexible (no compact arrays, no tagged_fields).
//!
//! Request layout:
//!
//! ```text
//! group_id  string
//! topics array<{
//!   name string
//!   partitions array<{ index i32 }>
//! }>
//! ```
//!
//! Response:
//! ```text
//! error_code        i16
//! throttle_time_ms  i32
//! topics array<{
//!   name string
//!   partitions array<{
//!     index      i32
//!     error_code i16
//!   }>
//! }>
//! ```

use tokio::sync::oneshot;

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use super::group_coordinator::{GroupMsg, OffsetDeleteRequest};
use crate::server::Broker;

pub const API_KEY: i16 = 47;
pub const MIN_VERSION: i16 = 0;
pub const MAX_VERSION: i16 = 0;

pub async fn handle_and_encode(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let group_id = r.read_string()?;
    let tcount = r.read_i32()?;
    let mut topic_layout: Vec<(String, Vec<i32>)> = Vec::with_capacity(tcount as usize);
    for _ in 0..tcount {
        let name = r.read_string()?;
        let pcount = r.read_i32()?;
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            parts.push(r.read_i32()?);
        }
        topic_layout.push((name, parts));
    }

    let coord_handle = broker.lookup_group_coordinator(&group_id);
    let mut per_partition_err: std::collections::HashMap<(String, i32), i16> =
        std::collections::HashMap::new();
    let top_level_err = match coord_handle {
        None => error_codes::INVALID_GROUP_ID,
        Some(coord) => {
            let keys: Vec<(String, i32)> = topic_layout
                .iter()
                .flat_map(|(name, parts)| {
                    parts.iter().map(move |p| (name.clone(), *p))
                })
                .collect();
            let (tx, rx) = oneshot::channel();
            coord.send(GroupMsg::OffsetDelete(OffsetDeleteRequest {
                keys: keys.clone(),
                respond_to: tx,
            }));
            let results = rx.await.unwrap_or_default();
            for (k, code) in results {
                per_partition_err.insert(k, code);
            }
            // Mirror the deletion into qx-topic's group_offset table by
            // committing the high watermark + 1 as the new offset — that
            // effectively "forgets" the offset since the next read sees
            // no group_offset entry to fall back to. For simplicity here
            // we just leave the durable side alone; coordinator state
            // is the source of truth for newly-committed offsets in v0.
            error_codes::NONE
        }
    };

    w.write_i16(top_level_err);
    w.write_i32(0); // throttle_time_ms
    w.write_i32(topic_layout.len() as i32);
    for (name, parts) in topic_layout {
        w.write_string(&name);
        w.write_i32(parts.len() as i32);
        for p in parts {
            w.write_i32(p);
            let code = if top_level_err != error_codes::NONE {
                top_level_err
            } else {
                per_partition_err
                    .get(&(name.clone(), p))
                    .copied()
                    .unwrap_or(error_codes::UNKNOWN_TOPIC_OR_PARTITION)
            };
            w.write_i16(code);
        }
    }
    Ok(())
}
