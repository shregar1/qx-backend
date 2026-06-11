//! Produce (api_key = 0), v9 only.
//!
//! Body shape on the wire:
//!
//! ```text
//! transactional_id  compact_nullable_string
//! acks              i16
//! timeout_ms        i32
//! topic_data        compact_array<TopicData>
//!   name            compact_string
//!   partition_data  compact_array<PartitionData>
//!     index         i32
//!     records       compact_nullable_bytes   (a record batch blob)
//!     tagged_fields
//!   tagged_fields
//! tagged_fields
//! ```
//!
//! For each `(topic, partition)` slot we decode the carried record batch,
//! verify its CRC32C, and route each record's value through `topic.send_to`.
//! The qx-assigned record id of the *first* successfully appended record
//! becomes the Kafka `base_offset` we return — subsequent records in the
//! batch land at sequential ids by construction (single-partition writer).
//!
//! Today: only uncompressed magic=2 batches; idempotent producer fields
//! parsed and ignored. Compression and producer-id sequence enforcement
//! are slated for a follow-up slice.

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use super::record_batch::{decode_batch, BatchError};
use crate::server::Broker;

pub const API_KEY: i16 = 0;
pub const MIN_VERSION: i16 = 9;
pub const MAX_VERSION: i16 = 9;

#[derive(Debug)]
pub struct ProduceRequest {
    #[allow(dead_code)] // transactions are not yet enforced
    pub transactional_id: Option<String>,
    #[allow(dead_code)] // we always durably append; acks=0/-1 makes no behavior difference yet
    pub acks: i16,
    #[allow(dead_code)] // ditto: no per-request timeout enforced
    pub timeout_ms: i32,
    pub topics: Vec<TopicProduceData>,
}

#[derive(Debug)]
pub struct TopicProduceData {
    pub name: String,
    pub partitions: Vec<PartitionProduceData>,
}

#[derive(Debug)]
pub struct PartitionProduceData {
    pub index: i32,
    /// Raw record-batch bytes as a refcounted slice into the originating
    /// request frame. Cloning is a refcount bump; the writer task
    /// receives this without a memcpy.
    pub records: Option<bytes::Bytes>,
}

impl ProduceRequest {
    /// Decode a Produce request body. `buf` is owned (refcount only)
    /// so partition record-batch bodies can be sliced out as `Bytes`
    /// views into the same allocation — no per-partition memcpy.
    pub fn decode(_version: i16, buf: bytes::Bytes) -> WireResult<Self> {
        let mut r = Reader::with_bytes(&buf);
        let transactional_id = r.read_compact_nullable_string()?;
        let acks = r.read_i16()?;
        let timeout_ms = r.read_i32()?;

        let n = r.read_uvarint()?;
        let topic_count = if n == 0 { 0 } else { n - 1 };
        let mut topics = Vec::with_capacity(topic_count as usize);
        for _ in 0..topic_count {
            let name = r.read_compact_string()?;
            let pn = r.read_uvarint()?;
            let part_count = if pn == 0 { 0 } else { pn - 1 };
            let mut partitions = Vec::with_capacity(part_count as usize);
            for _ in 0..part_count {
                let index = r.read_i32()?;
                // Zero-copy: slice into the request frame.
                let records = r.read_compact_bytes_slice("partition records")?;
                r.read_tagged_fields()?;
                partitions.push(PartitionProduceData { index, records });
            }
            r.read_tagged_fields()?;
            topics.push(TopicProduceData { name, partitions });
        }
        r.read_tagged_fields()?;

        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topics,
        })
    }
}

/// Handle a Produce request and write the response body into `w`.
///
/// Async because the per-partition group-commit writer is mpsc+oneshot
/// based — we send the request through the writer's inbox and await its
/// reply. Several concurrent Produces against the same partition get
/// drained into one redb commit.
/// Cumulative time spent in the produce hot path, broken down by phase.
/// Gated behind `QX_PRODUCE_INSTRUMENT=1` so it's a no-op on the normal
/// release path. The profile binary in `examples/produce_profile.rs`
/// sets the env var and calls `dump_produce_timings()` at the end.
///
/// The atomic increment is cheap (~3 ns on Apple Silicon), but
/// removing it from the hot path entirely when not needed avoids any
/// risk of perturbing measurements taken with a *different* profiler.
struct ProduceTimings {
    total_ns: std::sync::atomic::AtomicU64,
    fanout_ns: std::sync::atomic::AtomicU64,
    writer_await_ns: std::sync::atomic::AtomicU64,
    calls: std::sync::atomic::AtomicU64,
}

static PRODUCE_TIMINGS: ProduceTimings = ProduceTimings {
    total_ns: std::sync::atomic::AtomicU64::new(0),
    fanout_ns: std::sync::atomic::AtomicU64::new(0),
    writer_await_ns: std::sync::atomic::AtomicU64::new(0),
    calls: std::sync::atomic::AtomicU64::new(0),
};

fn produce_instrument_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("QX_PRODUCE_INSTRUMENT").is_ok())
}

/// Translate a writer-side error string into the appropriate Kafka
/// protocol error code. The writer surfaces the
/// `qx_topic::TopicError::Other(String)` it received from the
/// Replicator; when the underlying cause was `ForwardToLeader` the
/// right Kafka error is `NOT_LEADER_OR_FOLLOWER` (6) so that the
/// client refreshes Metadata and retries against the actual leader.
/// Anything else falls through to `UNKNOWN_SERVER_ERROR` as before.
fn kafka_err_code_for(msg: &str) -> i16 {
    if msg.contains("ForwardToLeader") || msg.contains("forward request to") {
        error_codes::NOT_LEADER_OR_FOLLOWER
    } else {
        error_codes::UNKNOWN_SERVER_ERROR
    }
}

/// Dump the cumulative breakdown captured under `QX_PRODUCE_INSTRUMENT`.
/// Used by the `produce_profile` example binary. Safe to call when
/// instrumentation was disabled — prints a no-data message and returns.
#[doc(hidden)]
pub fn dump_produce_timings() {
    use std::sync::atomic::Ordering::Relaxed;
    let calls = PRODUCE_TIMINGS.calls.swap(0, Relaxed);
    let total = PRODUCE_TIMINGS.total_ns.swap(0, Relaxed);
    let fanout = PRODUCE_TIMINGS.fanout_ns.swap(0, Relaxed);
    let writer = PRODUCE_TIMINGS.writer_await_ns.swap(0, Relaxed);
    if calls == 0 {
        eprintln!("produce-timings: no calls observed (was QX_PRODUCE_INSTRUMENT set?)");
        return;
    }
    let mean_us = |n: u64| n as f64 / calls as f64 / 1000.0;
    eprintln!(
        "produce-timings (mean per call over {calls} calls):\n  total handler:  {:>8.2} µs\n  partition fanout (join_all): {:>8.2} µs\n  writer await:   {:>8.2} µs",
        mean_us(total),
        mean_us(fanout),
        mean_us(writer),
    );
}

pub async fn handle_and_encode(
    w: &mut Writer,
    broker: &Broker,
    req: ProduceRequest,
    batch_window: std::time::Duration,
) {
    let _handler_start = if produce_instrument_enabled() {
        Some(std::time::Instant::now())
    } else {
        None
    };
    // Tried auto-bumping the batching window to 100 µs in Hyper mode
    // to coalesce concurrent producers. Empirically the bench shape
    // (single burst per iteration, then wait) made the window pure
    // overhead — each partition shard spent 100 µs waiting for
    // requests that never arrived. Reverted; the window stays at
    // whatever the operator sets explicitly. Production workloads
    // with steady-state concurrent producers may still benefit from
    // setting `produce_batch_window_micros` to a non-zero value.
    w.write_compact_array_len(req.topics.len() as u32);

    for topic in req.topics {
        w.write_compact_string(&topic.name);
        w.write_compact_array_len(topic.partitions.len() as u32);

        // Spawn every partition's produce future concurrently — each
        // partition is backed by an independent writer task, so polling
        // them in parallel lets multiple partitions' redb commits run
        // simultaneously instead of serializing 5 ms each through this
        // handler.
        let fanout_start = if produce_instrument_enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let futs: Vec<_> = topic
            .partitions
            .iter()
            .map(|part| {
                // `clone()` on Option<Bytes> is a refcount bump only.
                produce_one_partition(
                    broker,
                    &topic.name,
                    part.index,
                    part.records.clone(),
                    batch_window,
                )
            })
            .collect();
        let results = futures::future::join_all(futs).await;
        if let Some(start) = fanout_start {
            PRODUCE_TIMINGS.fanout_ns.fetch_add(
                start.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        for (part, (error_code, base_offset, error_message)) in
            topic.partitions.iter().zip(results)
        {
            w.write_i32(part.index);
            w.write_i16(error_code);
            w.write_i64(base_offset);
            w.write_i64(-1); // log_append_time_ms — we don't track wall time
            w.write_i64(0); // log_start_offset — no compaction yet
            // record_errors: empty compact array
            w.write_compact_array_len(0);
            w.write_compact_nullable_string(error_message.as_deref());
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }

    w.write_i32(0); // throttle_time_ms
    w.write_empty_tagged_fields();
    if let Some(start) = _handler_start {
        PRODUCE_TIMINGS.total_ns.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        PRODUCE_TIMINGS
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn produce_one_partition(
    broker: &Broker,
    topic_name: &str,
    partition: i32,
    records: Option<bytes::Bytes>,
    batch_window: std::time::Duration,
) -> (i16, i64, Option<String>) {
    let Some(records) = records else {
        return (error_codes::NONE, 0, None);
    };
    if records.is_empty() {
        return (error_codes::NONE, 0, None);
    }

    if broker.lookup(topic_name).is_none() {
        return (
            error_codes::UNKNOWN_TOPIC_OR_PARTITION,
            -1,
            Some(format!("topic '{topic_name}' not found")),
        );
    }

    if partition < 0 {
        return (error_codes::INVALID_REQUEST, -1, Some("negative partition".into()));
    }

    let mut cursor = 0usize;
    let mut base_offset: Option<i64> = None;
    let mut first_err: Option<(i16, String)> = None;

    while cursor < records.len() {
        // `Bytes::slice(start..)` is a refcount bump — the producer
        // can pack multiple batches in one frame and we walk them
        // without copying any payload bytes.
        let (mut batch, used) = match decode_batch(records.slice(cursor..)) {
            Ok(pair) => pair,
            Err(e) => {
                first_err = Some(batch_err_to_kafka(e));
                break;
            }
        };
        cursor += used;

        // Idempotent-producer dedup. -1 means non-idempotent; let it through.
        if batch.producer_id != -1 && batch.base_sequence >= 0 {
            if let Some(prev) = broker.producer_seen(batch.producer_id, partition) {
                if batch.base_sequence == prev.last_base_sequence {
                    // Exact retry — return what we returned last time. Spec
                    // says use NONE so the producer treats this as success.
                    if base_offset.is_none() {
                        base_offset = Some(prev.last_base_offset);
                    }
                    continue;
                } else if batch.base_sequence < prev.last_base_sequence {
                    // Older sequence — also a duplicate.
                    if base_offset.is_none() {
                        base_offset = Some(prev.last_base_offset);
                    }
                    continue;
                } else if batch.base_sequence
                    != prev.last_base_sequence + prev.last_record_count
                {
                    first_err = Some((
                        error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                        format!(
                            "expected sequence {}, got {}",
                            prev.last_base_sequence + prev.last_record_count,
                            batch.base_sequence
                        ),
                    ));
                    break;
                }
            } else if batch.base_sequence != 0 {
                // First time we see this producer but they think they're
                // in the middle of a sequence. Could be a restart; for
                // strict behavior, reject. Real Kafka returns UNKNOWN_PRODUCER_ID
                // (59). We'll surface as out-of-order for now.
                first_err = Some((
                    error_codes::OUT_OF_ORDER_SEQUENCE_NUMBER,
                    format!(
                        "first batch for producer {} has non-zero base_sequence {}",
                        batch.producer_id, batch.base_sequence
                    ),
                ));
                break;
            }
        }

        let mut batch_base_offset: Option<i64> = None;
        let mut batch_last_offset: Option<i64> = None;
        let record_count = batch.records.len() as i32;

        // Route through the per-partition group-commit writer. Multiple
        // concurrent Produces against the same partition land in one
        // redb commit instead of serializing behind the topic mutex.
        //
        // Move each record's value out as a `Bytes` — reference-counted
        // slice. Crossing the writer's mpsc boundary becomes a
        // refcount move, not a memcpy. For Phase 1 the per-record
        // allocation still happens inside the decoder; Phase 2 will
        // lift it into a shared-buffer slice and eliminate even that.
        let payloads_owned: Vec<bytes::Bytes> = std::mem::take(&mut batch.records)
            .into_iter()
            .map(|r| r.value.unwrap_or_default())
            .collect();
        // Total bytes attributed to this partition's batch — used
        // below to bump the per-topic metrics counters once we know
        // the commit succeeded.
        let attempted_msg_count = payloads_owned.len() as u64;
        let attempted_byte_count: u64 =
            payloads_owned.iter().map(|b| b.len() as u64).sum();
        let send_result: Result<Vec<u64>, (i16, String)> = if payloads_owned.is_empty() {
            Ok(Vec::new())
        } else if let Some(pool) = broker.shard_pool() {
            // Hyper mode: every Produce lands on a dedicated OS thread
            // pinned to a CPU core. No spawn_blocking, no work-
            // stealing scheduler. The shard's apply path commits
            // inline on its own thread.
            let topic_handle = match broker.lookup(topic_name) {
                Some(t) => t,
                None => {
                    first_err = Some((
                        error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                        format!("topic '{topic_name}' not found"),
                    ));
                    break;
                }
            };
            if partition < 0 || (partition as u32) >= topic_handle.partition_count() {
                Err((
                    error_codes::NOT_LEADER_OR_FOLLOWER,
                    format!("partition {partition} out of range"),
                ))
            } else {
                let rx = pool.submit(
                    topic_handle,
                    partition as u32,
                    payloads_owned,
                    batch_window,
                );
                match rx.await {
                    Ok(Ok(ids)) => Ok(ids),
                    Ok(Err(msg)) => Err((kafka_err_code_for(&msg), msg)),
                    Err(_) => Err((
                        error_codes::UNKNOWN_SERVER_ERROR,
                        "shard responder dropped".to_string(),
                    )),
                }
            }
        } else {
            // Standard mode: existing tokio per-partition writer task.
            match broker.produce_writer(topic_name, partition as u32, batch_window) {
                None => Err((
                    error_codes::NOT_LEADER_OR_FOLLOWER,
                    format!("partition {partition} out of range"),
                )),
                Some(writer) => {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    writer.send(crate::kafka::produce_writer::WriteRequest {
                        payloads: payloads_owned,
                        respond_to: tx,
                    });
                    let await_start = if produce_instrument_enabled() {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let result = match rx.await {
                        Ok(Ok(ids)) => Ok(ids),
                        Ok(Err(msg)) => Err((kafka_err_code_for(&msg), msg)),
                        Err(_) => Err((
                            error_codes::UNKNOWN_SERVER_ERROR,
                            "writer responder dropped".to_string(),
                        )),
                    };
                    if let Some(start) = await_start {
                        PRODUCE_TIMINGS.writer_await_ns.fetch_add(
                            start.elapsed().as_nanos() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    result
                }
            }
        };
        match send_result {
            Ok(ids) => {
                if let Some(first) = ids.first() {
                    batch_base_offset = Some(*first as i64);
                    if base_offset.is_none() {
                        base_offset = Some(*first as i64);
                    }
                }
                if let Some(last) = ids.last() {
                    batch_last_offset = Some(*last as i64);
                }
                // Bookkeeping for the admin API: every committed
                // payload contributes to this topic's running
                // msg/byte totals. Done on the success branch so a
                // rejected batch doesn't inflate the rate.
                broker.record_produce_success(
                    topic_name,
                    attempted_msg_count,
                    attempted_byte_count,
                );
            }
            Err(err) => {
                first_err = Some(err);
            }
        }

        if first_err.is_some() {
            break;
        }

        if batch.producer_id != -1 && batch.base_sequence >= 0 {
            if let Some(bo) = batch_base_offset {
                broker.record_producer(
                    batch.producer_id,
                    partition,
                    crate::kafka::ProducerSequenceState {
                        last_base_sequence: batch.base_sequence,
                        last_record_count: record_count,
                        last_base_offset: bo,
                    },
                );
            }
        }

        // If this batch is transactional, register the appended offsets
        // with the txn registry so they stay hidden from READ_COMMITTED
        // consumers until the producer's EndTxn arrives.
        if batch.is_transactional && batch.producer_id != -1 {
            if let (Some(first), Some(last)) = (batch_base_offset, batch_last_offset) {
                broker.txn_registry().record_in_flight(
                    topic_name,
                    partition,
                    batch.producer_id,
                    first as u64,
                    last as u64,
                );
            }
        }
    }

    if let Some((code, msg)) = first_err {
        return (code, -1, Some(msg));
    }

    (error_codes::NONE, base_offset.unwrap_or(0), None)
}

fn batch_err_to_kafka(e: BatchError) -> (i16, String) {
    match e {
        BatchError::UnsupportedMagic(_) | BatchError::UnsupportedCompression(_) => {
            (error_codes::UNSUPPORTED_FOR_MESSAGE_FORMAT, e.to_string())
        }
        BatchError::BadCrc { .. } | BatchError::Decompress { .. } => {
            (error_codes::INVALID_REQUEST, e.to_string())
        }
        BatchError::LengthMismatch { .. } | BatchError::Wire(_) => {
            (error_codes::INVALID_REQUEST, e.to_string())
        }
    }
}
