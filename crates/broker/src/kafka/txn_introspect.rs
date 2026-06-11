//! DescribeProducers (61, v0), DescribeTransactions (65, v0),
//! ListTransactions (66, v0).
//!
//! Read-only views into the transaction registry. `kafka-transactions.sh`
//! and the AdminClient transaction-management surface hit these.
//!
//! All three are v0 only and flexible.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use super::transactions::TxnRegistry;
use crate::server::Broker;

// ─────────────────────── DescribeProducers (61) v0 ──────────────────────

pub const DESCRIBE_PRODUCERS_KEY: i16 = 61;
pub const DESCRIBE_PRODUCERS_MIN: i16 = 0;
pub const DESCRIBE_PRODUCERS_MAX: i16 = 0;

/// Request layout (flexible):
///
/// ```text
/// topics compact_array<{
///   name compact_string
///   partitions compact_array<i32>
///   tagged_fields
/// }>
/// tagged_fields
/// ```
pub fn handle_describe_producers(
    w: &mut Writer,
    broker: &Broker,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let tcount = if n == 0 { 0 } else { n - 1 };
    let mut request: Vec<(String, Vec<i32>)> = Vec::with_capacity(tcount as usize);
    for _ in 0..tcount {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            parts.push(r.read_i32()?);
        }
        r.read_tagged_fields()?;
        request.push((name, parts));
    }
    r.read_tagged_fields()?;

    let registry = broker.txn_registry();

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(request.len() as u32);
    for (name, parts) in request {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        let topic_handle = broker.lookup(&name);
        for pidx in parts {
            let (code, producers) = match &topic_handle {
                None => (error_codes::UNKNOWN_TOPIC_OR_PARTITION, Vec::new()),
                Some(t) => {
                    if pidx < 0 || (pidx as u32) >= t.partition_count() {
                        (error_codes::UNKNOWN_TOPIC_OR_PARTITION, Vec::new())
                    } else {
                        (
                            error_codes::NONE,
                            registry.snapshot_producers_for_partition(&name, pidx),
                        )
                    }
                }
            };
            w.write_i32(pidx);
            w.write_i16(code);
            w.write_compact_nullable_string(None); // error_message
            w.write_compact_array_len(producers.len() as u32);
            for p in producers {
                w.write_i64(p.producer_id);
                w.write_i32(0);   // producer_epoch — we don't differentiate
                w.write_i32(-1);  // last_sequence — not tracked
                w.write_i64(-1);  // last_timestamp — not tracked
                w.write_i32(-1);  // coordinator_epoch
                w.write_i64(p.first_offset); // current_txn_start_offset
                w.write_empty_tagged_fields();
            }
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────── ListTransactions (66) v0 ───────────────────────

pub const LIST_TRANSACTIONS_KEY: i16 = 66;
pub const LIST_TRANSACTIONS_MIN: i16 = 0;
pub const LIST_TRANSACTIONS_MAX: i16 = 0;

/// Request layout (flexible):
///
/// ```text
/// state_filters    compact_array<compact_string>
/// producer_id_filters compact_array<i64>
/// tagged_fields
/// ```
pub fn handle_list_transactions(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let sn = r.read_uvarint()?;
    let state_count = if sn == 0 { 0 } else { sn - 1 };
    let mut state_filter: Vec<String> = Vec::with_capacity(state_count as usize);
    for _ in 0..state_count {
        state_filter.push(r.read_compact_string()?);
    }
    let pn = r.read_uvarint()?;
    let pid_count = if pn == 0 { 0 } else { pn - 1 };
    let mut pid_filter: Vec<i64> = Vec::with_capacity(pid_count as usize);
    for _ in 0..pid_count {
        pid_filter.push(r.read_i64()?);
    }
    r.read_tagged_fields()?;

    let mut all = registry.snapshot_transactions();
    if !state_filter.is_empty() {
        all.retain(|t| state_filter.iter().any(|s| s == t.state));
    }
    if !pid_filter.is_empty() {
        all.retain(|t| pid_filter.contains(&t.producer_id));
    }

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    // unknown_state_filters: any states we don't recognize. We accept all
    // strings, so return empty.
    w.write_compact_array_len(0);
    w.write_compact_array_len(all.len() as u32);
    for t in all {
        w.write_compact_string(&t.transactional_id);
        w.write_i64(t.producer_id);
        w.write_compact_string(t.state);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────── DescribeTransactions (65) v0 ──────────────────

pub const DESCRIBE_TRANSACTIONS_KEY: i16 = 65;
pub const DESCRIBE_TRANSACTIONS_MIN: i16 = 0;
pub const DESCRIBE_TRANSACTIONS_MAX: i16 = 0;

/// Request layout (flexible):
///
/// ```text
/// transactional_ids compact_array<compact_string>
/// tagged_fields
/// ```
pub fn handle_describe_transactions(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    let mut ids: Vec<String> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        ids.push(r.read_compact_string()?);
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(ids.len() as u32);
    for tid in ids {
        match registry.snapshot_one(&tid) {
            None => {
                w.write_i16(error_codes::INVALID_REQUEST);
                w.write_compact_string(&tid);
                w.write_compact_string("Unknown");
                w.write_i64(-1); // timeout_ms
                w.write_i64(-1); // start_time_ms
                w.write_i64(-1); // producer_id
                w.write_i16(-1); // producer_epoch
                w.write_compact_array_len(0); // topics
                w.write_empty_tagged_fields();
            }
            Some(snap) => {
                // Group partitions by topic for the response.
                let mut by_topic: std::collections::BTreeMap<String, Vec<i32>> =
                    std::collections::BTreeMap::new();
                for (t, p) in snap.partitions {
                    by_topic.entry(t).or_default().push(p);
                }

                w.write_i16(error_codes::NONE);
                w.write_compact_string(&snap.transactional_id);
                w.write_compact_string(snap.state);
                w.write_i64(snap.timeout_ms);
                w.write_i64(-1); // start_time_ms — we don't record it
                w.write_i64(snap.producer_id);
                w.write_i16(snap.producer_epoch);
                w.write_compact_array_len(by_topic.len() as u32);
                for (topic, parts) in by_topic {
                    w.write_compact_string(&topic);
                    w.write_compact_array_len(parts.len() as u32);
                    for p in parts {
                        w.write_i32(p);
                    }
                    w.write_empty_tagged_fields();
                }
                w.write_empty_tagged_fields();
            }
        }
    }
    w.write_empty_tagged_fields();
    Ok(())
}
