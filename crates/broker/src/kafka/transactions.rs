//! Transactional producer / consumer support.
//!
//! Within a broker lifetime this provides **real** transactional semantics:
//!   - Records produced inside an open transaction are tracked per
//!     `(topic, partition)` as "in-flight".
//!   - `Fetch` with `isolation_level=READ_COMMITTED` truncates the
//!     response at the partition's Last Stable Offset (the smallest
//!     in-flight offset across all open transactions) so uncommitted
//!     records are hidden.
//!   - On commit, in-flight records become readable.
//!   - On abort, the offset range is added to the partition's
//!     `aborted_transactions` list, surfaced verbatim in Fetch responses
//!     so clients drop the corresponding records.
//!
//! Not yet covered (would need storage-layer changes):
//!   - Persistence across broker restart. A restart while a transaction is
//!     open re-exposes the in-flight records.
//!   - Inter-partition atomicity isn't transactional across multiple
//!     brokers since we're single-broker.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use super::txn_store::{PersistedAborted, PersistedInFlight, PersistedOffsetCommit, PersistedProducer, TxnStore};

#[derive(Debug, Default)]
pub struct TxnRegistry {
    inner: Mutex<TxnInner>,
    /// Optional persistent backing. `None` means in-memory only (used by
    /// unit tests that don't care about restart). `Some` is the
    /// production path — every state mutation also writes to redb so a
    /// broker restart mid-transaction recovers the visibility picture.
    store: Option<Arc<TxnStore>>,
}

#[derive(Debug, Default)]
struct TxnInner {
    next_producer_id: i64,
    /// `transactional_id` → state
    by_txn_id: HashMap<String, TxnState>,
    /// Idempotent (no transactional_id) producers — just track sequence.
    /// In-memory counter for distributing fresh producer_ids.
    next_idempotent: i64,
    /// In-flight transactional record ranges, indexed by `(topic, partition)`.
    /// Each entry is `(producer_id, first_offset, last_offset)`. A record
    /// is "in-flight" from the moment Produce appends it through commit /
    /// abort. Fetch with READ_COMMITTED uses the minimum first_offset
    /// here as the partition's Last Stable Offset.
    in_flight_by_partition: HashMap<(String, i32), Vec<InFlightRange>>,
    /// Closed-but-aborted transactional record ranges. Surfaced verbatim
    /// in Fetch's `aborted_transactions` array so clients filter them out.
    /// Keyed by `(topic, partition)`; entries store `(producer_id,
    /// first_offset)` per Kafka's wire shape.
    aborted_by_partition: HashMap<(String, i32), Vec<AbortedTxn>>,
}

#[derive(Debug, Clone)]
struct TxnState {
    producer_id: i64,
    producer_epoch: i16,
    partitions: HashSet<(String, i32)>,
    phase: Phase,
    /// Wall-clock instant after which an open txn is considered expired
    /// and auto-aborted by the periodic sweep.
    timeout_at: Option<std::time::Instant>,
    /// How long this producer asked for. Used to refresh `timeout_at` on
    /// every AddPartitionsToTxn (mirrors real Kafka).
    timeout: std::time::Duration,
    /// Pending consumer-side offset commits, staged by `TxnOffsetCommit`.
    /// Applied to the group coordinator on `EndTxn(commit)`, dropped on
    /// abort — this is the consumer-side half of exactly-once.
    pending_offsets: Vec<PendingOffsetCommit>,
}

/// One staged consumer-side offset commit awaiting transaction outcome.
#[derive(Debug, Clone)]
pub struct PendingOffsetCommit {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Copy)]
struct InFlightRange {
    producer_id: i64,
    first_offset: u64,
    last_offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct AbortedTxn {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone)]
pub struct TxnSnapshot {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// One of "Empty", "Ongoing", "CompleteCommit", "CompleteAbort".
    /// Matches Kafka's `TransactionState` strings.
    pub state: &'static str,
    pub partitions: Vec<(String, i32)>,
    pub timeout_ms: i64,
    /// Milliseconds until auto-abort. `-1` when no deadline.
    pub timeout_remaining_ms: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct ProducerSnapshot {
    pub producer_id: i64,
    pub first_offset: i64,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Empty,
    Ongoing,
    Completed,
}

impl TxnRegistry {
    /// Open a persistent registry rooted at `dir`. Reloads producers,
    /// in-flight ranges, and aborted ranges from redb so a broker restart
    /// mid-transaction picks up where it left off.
    pub fn open(dir: impl AsRef<std::path::Path>) -> crate::error::Result<Self> {
        let store = TxnStore::open(dir)?;
        let mut inner = TxnInner::default();

        // Reload producers.
        let mut max_pid = 0i64;
        for (tid, p) in store.all_producers()? {
            max_pid = max_pid.max(p.producer_id);
            inner.by_txn_id.insert(
                tid,
                TxnState {
                    producer_id: p.producer_id,
                    producer_epoch: p.producer_epoch,
                    partitions: HashSet::new(),
                    phase: Phase::Empty, // refined below if there's in-flight
                    timeout_at: None,
                    timeout: std::time::Duration::from_millis(p.timeout_ms),
                    pending_offsets: p
                        .pending_offsets
                        .into_iter()
                        .map(|po| PendingOffsetCommit {
                            group_id: po.group_id,
                            topic: po.topic,
                            partition: po.partition,
                            offset: po.offset,
                        })
                        .collect(),
                },
            );
        }
        inner.next_producer_id = max_pid;

        // Reload in-flight ranges and back-fill TxnState.partitions /
        // phase so the loaded producers look exactly like they did
        // before the restart.
        for ((topic, partition), range) in store.all_in_flight()? {
            inner
                .in_flight_by_partition
                .entry((topic.clone(), partition))
                .or_default()
                .push(InFlightRange {
                    producer_id: range.producer_id,
                    first_offset: range.first_offset,
                    last_offset: range.last_offset,
                });
            // Mark the producer as Ongoing if we can find them.
            for state in inner.by_txn_id.values_mut() {
                if state.producer_id == range.producer_id {
                    state.phase = Phase::Ongoing;
                    state.partitions.insert((topic.clone(), partition));
                }
            }
        }

        // Reload aborted ranges.
        for ((topic, partition), aborted) in store.all_aborted()? {
            inner
                .aborted_by_partition
                .entry((topic, partition))
                .or_default()
                .push(AbortedTxn {
                    producer_id: aborted.producer_id,
                    first_offset: aborted.first_offset,
                });
        }

        Ok(Self {
            inner: Mutex::new(inner),
            store: Some(Arc::new(store)),
        })
    }

    /// Either fetch the existing producer state for `transactional_id`
    /// (bumping its epoch) or allocate a new one. Idempotent-only
    /// producers (no txn_id) get a fresh id each call.
    pub fn init_producer(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
    ) -> (i64, i16) {
        let mut inner = self.inner.lock().unwrap();
        let timeout = std::time::Duration::from_millis(
            transaction_timeout_ms.max(1_000).min(900_000) as u64,
        );
        match transactional_id {
            None => {
                // Idempotent-only producer: hand out a stable id from a
                // separate counter. (Real Kafka tracks these in a coord
                // log; we just use an in-memory sequence.)
                inner.next_idempotent += 1;
                (1_000_000 + inner.next_idempotent, 0)
            }
            Some(tid) => {
                if let Some(state) = inner.by_txn_id.get_mut(tid) {
                    // Same producer reconnecting: bump epoch, reset txn.
                    state.producer_epoch = state.producer_epoch.wrapping_add(1);
                    state.partitions.clear();
                    state.phase = Phase::Empty;
                    state.timeout = timeout;
                    state.timeout_at = None;
                    let pid = state.producer_id;
                    let epoch = state.producer_epoch;
                    let pending: Vec<PersistedOffsetCommit> = state
                        .pending_offsets
                        .iter()
                        .map(|p| PersistedOffsetCommit {
                            group_id: p.group_id.clone(),
                            topic: p.topic.clone(),
                            partition: p.partition,
                            offset: p.offset,
                        })
                        .collect();
                    if let Some(store) = self.store.as_ref() {
                        let _ = store.put_producer(
                            tid,
                            &PersistedProducer {
                                producer_id: pid,
                                producer_epoch: epoch,
                                timeout_ms: timeout.as_millis() as u64,
                                pending_offsets: pending,
                            },
                        );
                    }
                    return (pid, epoch);
                }
                inner.next_producer_id += 1;
                let pid = inner.next_producer_id;
                let state = TxnState {
                    producer_id: pid,
                    producer_epoch: 0,
                    partitions: HashSet::new(),
                    phase: Phase::Empty,
                    timeout_at: None,
                    timeout,
                    pending_offsets: Vec::new(),
                };
                inner.by_txn_id.insert(tid.to_string(), state);
                if let Some(store) = self.store.as_ref() {
                    let _ = store.put_producer(
                        tid,
                        &PersistedProducer {
                            producer_id: pid,
                            producer_epoch: 0,
                            timeout_ms: timeout.as_millis() as u64,
                            pending_offsets: Vec::new(),
                        },
                    );
                }
                (pid, 0)
            }
        }
    }

    /// Stash a consumer offset commit inside an open transaction. Returns
    /// `error_codes::NONE` if the txn is open + producer fencing checks
    /// out, otherwise an error code the caller surfaces back to the
    /// client. The offset is **not** applied to the group coordinator
    /// yet — that happens on `end_txn(commit=true)`.
    pub fn stash_offset(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        commit: PendingOffsetCommit,
    ) -> i16 {
        let snapshot = {
            let mut inner = self.inner.lock().unwrap();
            match inner.by_txn_id.get_mut(transactional_id) {
                Some(state)
                    if state.producer_id == producer_id
                        && state.producer_epoch == producer_epoch =>
                {
                    state.pending_offsets.push(commit);
                    Some((
                        state.producer_id,
                        state.producer_epoch,
                        state.timeout.as_millis() as u64,
                        state
                            .pending_offsets
                            .iter()
                            .map(|p| PersistedOffsetCommit {
                                group_id: p.group_id.clone(),
                                topic: p.topic.clone(),
                                partition: p.partition,
                                offset: p.offset,
                            })
                            .collect::<Vec<_>>(),
                    ))
                }
                Some(_) | None => return error_codes::INVALID_REQUEST,
            }
        };
        if let (Some((pid, epoch, tms, pending)), Some(store)) = (snapshot, self.store.as_ref()) {
            let _ = store.put_producer(
                transactional_id,
                &PersistedProducer {
                    producer_id: pid,
                    producer_epoch: epoch,
                    timeout_ms: tms,
                    pending_offsets: pending,
                },
            );
        }
        error_codes::NONE
    }

    /// Record that `producer_id` has added these partitions to its
    /// in-flight transaction. Returns per-partition error codes — all NONE
    /// in our stub.
    pub fn add_partitions(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        topic: &str,
        partitions: &[i32],
    ) -> Vec<i16> {
        let mut inner = self.inner.lock().unwrap();
        match inner.by_txn_id.get_mut(transactional_id) {
            Some(state) if state.producer_id == producer_id
                && state.producer_epoch == producer_epoch =>
            {
                for &p in partitions {
                    state.partitions.insert((topic.to_string(), p));
                }
                state.phase = Phase::Ongoing;
                // Refresh the timeout window — this is the
                // "transaction started / extended" moment per the protocol.
                state.timeout_at = Some(std::time::Instant::now() + state.timeout);
                vec![error_codes::NONE; partitions.len()]
            }
            Some(_) => vec![error_codes::INVALID_REQUEST; partitions.len()],
            None => vec![error_codes::INVALID_REQUEST; partitions.len()],
        }
    }

    /// Snapshot every transactional producer for introspection (Describe
    /// Transactions, ListTransactions). Returns
    /// `(transactional_id, producer_id, epoch, state, partitions,
    /// timeout_ms, timeout_remaining_ms)` tuples.
    pub fn snapshot_transactions(&self) -> Vec<TxnSnapshot> {
        let inner = self.inner.lock().unwrap();
        let now = std::time::Instant::now();
        inner
            .by_txn_id
            .iter()
            .map(|(tid, state)| {
                let state_name = match state.phase {
                    Phase::Empty => "Empty",
                    Phase::Ongoing => "Ongoing",
                    Phase::Completed => "CompleteCommit",
                };
                let remaining_ms = state
                    .timeout_at
                    .map(|d| {
                        if d > now {
                            (d - now).as_millis() as i64
                        } else {
                            0
                        }
                    })
                    .unwrap_or(-1);
                TxnSnapshot {
                    transactional_id: tid.clone(),
                    producer_id: state.producer_id,
                    producer_epoch: state.producer_epoch,
                    state: state_name,
                    partitions: state.partitions.iter().cloned().collect(),
                    timeout_ms: state.timeout.as_millis() as i64,
                    timeout_remaining_ms: remaining_ms,
                }
            })
            .collect()
    }

    /// Find one snapshot by `transactional_id`. Returns `None` for unknown.
    pub fn snapshot_one(&self, transactional_id: &str) -> Option<TxnSnapshot> {
        self.snapshot_transactions()
            .into_iter()
            .find(|s| s.transactional_id == transactional_id)
    }

    /// Producer-introspection view: every `(producer_id, partition)` we
    /// currently have an in-flight range for, paired with the range. Used
    /// by DescribeProducers.
    pub fn snapshot_producers_for_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Vec<ProducerSnapshot> {
        let inner = self.inner.lock().unwrap();
        let key = (topic.to_string(), partition);
        let Some(ranges) = inner.in_flight_by_partition.get(&key) else {
            return Vec::new();
        };
        ranges
            .iter()
            .map(|r| ProducerSnapshot {
                producer_id: r.producer_id,
                first_offset: r.first_offset as i64,
                last_offset: r.last_offset as i64,
            })
            .collect()
    }

    /// Verify that `(producer_id, producer_epoch)` matches the registered
    /// state for `transactional_id`. Used by AddOffsetsToTxn (which has no
    /// per-partition state to mutate) so we can fence stale producer
    /// generations before they stage offsets.
    pub fn validate_producer(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> i16 {
        let inner = self.inner.lock().unwrap();
        match inner.by_txn_id.get(transactional_id) {
            Some(state)
                if state.producer_id == producer_id
                    && state.producer_epoch == producer_epoch =>
            {
                error_codes::NONE
            }
            Some(_) => error_codes::INVALID_PRODUCER_EPOCH,
            None => error_codes::INVALID_PRODUCER_ID_MAPPING,
        }
    }

    /// Sweep open transactions whose `timeout_at` is in the past and
    /// auto-abort them. Returns the number of transactions aborted.
    /// Designed to be called from a background tokio task on a fixed
    /// cadence (every few seconds is fine — the producer's session
    /// timeout is typically ~1 minute).
    pub fn sweep_expired(&self) -> usize {
        let now = std::time::Instant::now();
        // Snapshot expired txns under the lock, then call end_txn() in
        // a second pass (it re-acquires the lock and handles cleanup).
        let expired: Vec<(String, i64, i16)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .by_txn_id
                .iter()
                .filter_map(|(tid, state)| {
                    let deadline = state.timeout_at?;
                    if state.phase == Phase::Ongoing && deadline < now {
                        Some((tid.clone(), state.producer_id, state.producer_epoch))
                    } else {
                        None
                    }
                })
                .collect()
        };
        let n = expired.len();
        for (tid, pid, epoch) in expired {
            let _ = self.end_txn(&tid, pid, epoch, false);
        }
        n
    }

    /// Backward-compatible wrapper that discards the pending offsets.
    /// New code should call [`end_txn_with_offsets`] so consumer-side
    /// `TxnOffsetCommit`s can be applied atomically with the transaction.
    pub fn end_txn(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> i16 {
        let (code, _) = self.end_txn_with_offsets(
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        );
        code
    }

    /// Commit or abort an in-flight transaction. On commit, in-flight
    /// records become visible to READ_COMMITTED consumers and any
    /// staged consumer offset commits are returned for application; on
    /// abort, records move to the partition's `aborted_transactions`
    /// list and the staged offsets are dropped.
    pub fn end_txn_with_offsets(
        &self,
        transactional_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> (i16, Vec<PendingOffsetCommit>) {
        let mut inner = self.inner.lock().unwrap();
        let pid = producer_id;
        let valid = match inner.by_txn_id.get(transactional_id) {
            Some(state)
                if state.producer_id == producer_id
                    && state.producer_epoch == producer_epoch =>
            {
                true
            }
            _ => false,
        };
        if !valid {
            return (error_codes::INVALID_REQUEST, Vec::new());
        }

        // Drain staged consumer offsets — applied on commit, dropped on
        // abort. Either way, clear the queue so a fresh txn starts empty.
        let pending: Vec<PendingOffsetCommit> = inner
            .by_txn_id
            .get_mut(transactional_id)
            .map(|s| std::mem::take(&mut s.pending_offsets))
            .unwrap_or_default();

        // Drain this producer's in-flight ranges across every partition.
        // Track what we moved so we can persist the same change.
        let keys: Vec<(String, i32)> =
            inner.in_flight_by_partition.keys().cloned().collect();
        let mut all_moved: Vec<((String, i32), InFlightRange)> = Vec::new();
        for key in keys {
            let mut moved: Vec<InFlightRange> = Vec::new();
            if let Some(ranges) = inner.in_flight_by_partition.get_mut(&key) {
                let (m, k): (Vec<_>, Vec<_>) =
                    ranges.drain(..).partition(|r| r.producer_id == pid);
                moved = m;
                *ranges = k;
            }
            if !committed && !moved.is_empty() {
                let entry = inner.aborted_by_partition.entry(key.clone()).or_default();
                for r in &moved {
                    entry.push(AbortedTxn {
                        producer_id: r.producer_id,
                        first_offset: r.first_offset as i64,
                    });
                }
            }
            for r in moved {
                all_moved.push((key.clone(), r));
            }
        }
        // Drop the partition lists entirely if empty so LSO computation
        // sees nothing in-flight.
        inner
            .in_flight_by_partition
            .retain(|_, v| !v.is_empty());

        let timeout_ms_for_persist;
        let producer_epoch_for_persist;
        if let Some(state) = inner.by_txn_id.get_mut(transactional_id) {
            state.partitions.clear();
            state.phase = Phase::Completed;
            // pending_offsets are already drained at the top of this fn.
            timeout_ms_for_persist = state.timeout.as_millis() as u64;
            producer_epoch_for_persist = state.producer_epoch;
        } else {
            timeout_ms_for_persist = 60_000;
            producer_epoch_for_persist = producer_epoch;
        }
        drop(inner); // release before redb writes

        if let Some(store) = self.store.as_ref() {
            // Persist the moved-out in-flight ranges (delete from store).
            let to_delete: Vec<((String, i32), PersistedInFlight)> = all_moved
                .iter()
                .map(|(k, r)| {
                    (
                        k.clone(),
                        PersistedInFlight {
                            producer_id: r.producer_id,
                            first_offset: r.first_offset,
                            last_offset: r.last_offset,
                        },
                    )
                })
                .collect();
            let _ = store.delete_in_flight_for_producer(pid, &to_delete);
            // Persist aborted entries.
            if !committed {
                for ((topic, partition), r) in &all_moved {
                    let _ = store.put_aborted(
                        topic,
                        *partition,
                        PersistedAborted {
                            producer_id: r.producer_id,
                            first_offset: r.first_offset as i64,
                        },
                    );
                }
            }
            // Persist updated producer (pending_offsets cleared, partitions cleared).
            let _ = store.put_producer(
                transactional_id,
                &PersistedProducer {
                    producer_id: pid,
                    producer_epoch: producer_epoch_for_persist,
                    timeout_ms: timeout_ms_for_persist,
                    pending_offsets: Vec::new(),
                },
            );
        }

        let to_apply = if committed { pending } else { Vec::new() };
        (error_codes::NONE, to_apply)
    }

    /// Called from Produce when a magic=2 batch arrives with the
    /// is_transactional bit set. Records the offset range so Fetch
    /// truncates above it for READ_COMMITTED consumers.
    pub fn record_in_flight(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        first_offset: u64,
        last_offset: u64,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .in_flight_by_partition
            .entry((topic.to_string(), partition))
            .or_default()
            .push(InFlightRange {
                producer_id,
                first_offset,
                last_offset,
            });
        if let Some(store) = self.store.as_ref() {
            let _ = store.put_in_flight(
                topic,
                partition,
                PersistedInFlight {
                    producer_id,
                    first_offset,
                    last_offset,
                },
            );
        }
    }

    /// Last Stable Offset for a `(topic, partition)`: the smallest
    /// in-flight first_offset, or `None` if there are no open
    /// transactions touching this partition. Fetch with READ_COMMITTED
    /// truncates at this value.
    pub fn last_stable_offset(&self, topic: &str, partition: i32) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        let key = (topic.to_string(), partition);
        inner
            .in_flight_by_partition
            .get(&key)?
            .iter()
            .map(|r| r.first_offset)
            .min()
    }

    /// Snapshot of aborted-transaction entries for a `(topic, partition)`
    /// whose `first_offset` falls within `[fetch_offset, end_offset)`.
    /// Surfaced verbatim in the Fetch response so clients can drop the
    /// corresponding records.
    pub fn aborted_in_range(
        &self,
        topic: &str,
        partition: i32,
        fetch_offset: i64,
        end_offset: i64,
    ) -> Vec<AbortedTxn> {
        let inner = self.inner.lock().unwrap();
        let key = (topic.to_string(), partition);
        let Some(v) = inner.aborted_by_partition.get(&key) else {
            return Vec::new();
        };
        v.iter()
            .filter(|a| a.first_offset >= fetch_offset && a.first_offset < end_offset)
            .copied()
            .collect()
    }
}

// ───────────────────────  InitProducerId (22) v4  ─────────────────────────

pub const INIT_PRODUCER_ID_KEY: i16 = 22;
pub const INIT_PRODUCER_ID_MIN: i16 = 4;
pub const INIT_PRODUCER_ID_MAX: i16 = 4;

pub fn handle_init_producer_id(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let transactional_id = r.read_compact_nullable_string()?;
    let transaction_timeout_ms = r.read_i32()?;
    let _producer_id = r.read_i64()?;
    let _producer_epoch = r.read_i16()?;
    r.read_tagged_fields()?;

    let (producer_id, producer_epoch) =
        registry.init_producer(transactional_id.as_deref(), transaction_timeout_ms);

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_i64(producer_id);
    w.write_i16(producer_epoch);
    w.write_empty_tagged_fields();
    Ok(())
}

// ───────────────────────  AddPartitionsToTxn (24) v3  ─────────────────────

pub const ADD_PARTITIONS_TO_TXN_KEY: i16 = 24;
pub const ADD_PARTITIONS_TO_TXN_MIN: i16 = 3;
pub const ADD_PARTITIONS_TO_TXN_MAX: i16 = 3;

pub fn handle_add_partitions_to_txn(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let transactional_id = r.read_compact_string()?;
    let producer_id = r.read_i64()?;
    let producer_epoch = r.read_i16()?;
    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };

    let mut topics_in = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            parts.push(r.read_i32()?);
        }
        r.read_tagged_fields()?;
        topics_in.push((name, parts));
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(topics_in.len() as u32);
    for (name, parts) in topics_in {
        let codes = registry.add_partitions(
            &transactional_id,
            producer_id,
            producer_epoch,
            &name,
            &parts,
        );
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for (idx, code) in parts.iter().zip(codes.iter()) {
            w.write_i32(*idx);
            w.write_i16(*code);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ───────────────────────  AddOffsetsToTxn (25) v3  ────────────────────────

pub const ADD_OFFSETS_TO_TXN_KEY: i16 = 25;
pub const ADD_OFFSETS_TO_TXN_MIN: i16 = 3;
pub const ADD_OFFSETS_TO_TXN_MAX: i16 = 3;

/// Tells the transaction coordinator that the producer is about to
/// commit consumer-group offsets as part of its transaction. The
/// coordinator pre-registers the group so subsequent `TxnOffsetCommit`
/// + `EndTxn` know which group to apply on commit.
///
/// Today we treat this as a no-op for state but still validate the
/// (producer_id, epoch) — any txn we have on file must match. The
/// real bookkeeping happens in `TxnOffsetCommit` itself, which stages
/// the offsets keyed by group.
///
/// Request layout (v3, flexible):
///
/// ```text
/// transactional_id compact_string
/// producer_id      i64
/// producer_epoch   i16
/// group_id         compact_string
/// tagged_fields
/// ```
///
/// Response:
/// ```text
/// throttle_time_ms i32
/// error_code       i16
/// tagged_fields
/// ```
pub fn handle_add_offsets_to_txn(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let transactional_id = r.read_compact_string()?;
    let producer_id = r.read_i64()?;
    let producer_epoch = r.read_i16()?;
    let _group_id = r.read_compact_string()?;
    r.read_tagged_fields()?;

    let code = registry.validate_producer(&transactional_id, producer_id, producer_epoch);

    w.write_i32(0); // throttle_time_ms
    w.write_i16(code);
    w.write_empty_tagged_fields();
    Ok(())
}

// ───────────────────────  EndTxn (26) v3  ─────────────────────────────────

pub const END_TXN_KEY: i16 = 26;
pub const END_TXN_MIN: i16 = 3;
pub const END_TXN_MAX: i16 = 3;

pub async fn handle_end_txn(
    w: &mut Writer,
    broker: &crate::server::Broker,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let transactional_id = r.read_compact_string()?;
    let producer_id = r.read_i64()?;
    let producer_epoch = r.read_i16()?;
    let committed = r.read_bool()?;
    r.read_tagged_fields()?;

    let (code, pending) = registry.end_txn_with_offsets(
        &transactional_id,
        producer_id,
        producer_epoch,
        committed,
    );

    // Apply staged consumer offsets through their respective group
    // coordinators. We send-and-await per group so the response can't
    // claim "committed" before the offsets are visible to OffsetFetch.
    if committed && !pending.is_empty() {
        use std::collections::HashMap;
        let mut by_group: HashMap<String, Vec<PendingOffsetCommit>> = HashMap::new();
        for entry in pending {
            by_group.entry(entry.group_id.clone()).or_default().push(entry);
        }
        for (group, entries) in by_group {
            let coord = broker.group_coordinator(&group);
            let (tx, rx) = tokio::sync::oneshot::channel();
            let offsets: Vec<_> = entries
                .iter()
                .map(|e| ((e.topic.clone(), e.partition), e.offset))
                .collect();
            coord.send(super::group_coordinator::GroupMsg::OffsetCommit(
                super::group_coordinator::OffsetCommitRequest {
                    member_id: String::new(),
                    generation_id: -1, // bypass fencing — the txn vouches
                    offsets,
                    respond_to: tx,
                },
            ));
            let _ = rx.await;

            // Mirror into qx-topic group_offset so native consumers see it.
            for e in &entries {
                if let Some(topic) = broker.lookup(&e.topic) {
                    let _ = topic.commit_offset(&group, e.partition as u32, e.offset as u64);
                }
            }
        }
    }

    w.write_i32(0); // throttle_time_ms
    w.write_i16(code);
    w.write_empty_tagged_fields();
    Ok(())
}

// ───────────────────────  WriteTxnMarkers (27) v0 — stub  ─────────────────

pub const WRITE_TXN_MARKERS_KEY: i16 = 27;
pub const WRITE_TXN_MARKERS_MIN: i16 = 0;
pub const WRITE_TXN_MARKERS_MAX: i16 = 0;

/// WriteTxnMarkers is typically broker-internal (one broker tells another
/// "I've decided this txn's outcome — write the marker to your replica").
/// In single-broker mode there's nothing to do; we just acknowledge.
pub fn handle_write_txn_markers(
    w: &mut Writer,
    body: &[u8],
) -> WireResult<()> {
    // v0 is non-flexible. Decode minimally to consume the request.
    let mut r = Reader::new(body);
    let marker_count = r.read_i32()?;
    let mut markers = Vec::with_capacity(marker_count.max(0) as usize);
    for _ in 0..marker_count {
        let producer_id = r.read_i64()?;
        let _producer_epoch = r.read_i16()?;
        let _transaction_result = r.read_bool()?;
        let topic_count = r.read_i32()?;
        let mut topics = Vec::with_capacity(topic_count.max(0) as usize);
        for _ in 0..topic_count {
            let name = r.read_string()?;
            let pcount = r.read_i32()?;
            let mut parts = Vec::with_capacity(pcount.max(0) as usize);
            for _ in 0..pcount {
                parts.push(r.read_i32()?);
            }
            topics.push((name, parts));
        }
        let _coordinator_epoch = r.read_i32()?;
        markers.push((producer_id, topics));
    }

    // Response is non-flexible v0.
    w.write_i32(markers.len() as i32);
    for (producer_id, topics) in markers {
        w.write_i64(producer_id);
        w.write_i32(topics.len() as i32);
        for (name, parts) in topics {
            w.write_string(&name);
            w.write_i32(parts.len() as i32);
            for p in parts {
                w.write_i32(p);
                w.write_i16(error_codes::NONE);
            }
        }
    }
    Ok(())
}

// ───────────────────────  TxnOffsetCommit (28) v3 — stub  ─────────────────

pub const TXN_OFFSET_COMMIT_KEY: i16 = 28;
pub const TXN_OFFSET_COMMIT_MIN: i16 = 3;
pub const TXN_OFFSET_COMMIT_MAX: i16 = 3;

/// Consumer-side transactional offset commit. Offsets are **staged** in
/// the transaction's pending list — they're applied to the group
/// coordinator only when the producer issues `EndTxn(commit=true)`. On
/// `EndTxn(commit=false)`, the stash is dropped. That's the consumer
/// half of exactly-once.
pub fn handle_txn_offset_commit(
    w: &mut Writer,
    registry: &TxnRegistry,
    body: &[u8],
) -> WireResult<()> {
    let mut r = Reader::new(body);
    let transactional_id = r.read_compact_string()?;
    let group_id = r.read_compact_string()?;
    let producer_id = r.read_i64()?;
    let producer_epoch = r.read_i16()?;
    let _generation_id = r.read_i32()?;
    let _member_id = r.read_compact_string()?;
    let _group_instance_id = r.read_compact_nullable_string()?;

    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    // Collect (topic, partition, offset) per-partition for the response.
    let mut layout: Vec<(String, Vec<(i32, i64)>)> = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            let pidx = r.read_i32()?;
            let offset = r.read_i64()?;
            let _leader_epoch = r.read_i32()?;
            let _metadata = r.read_compact_nullable_string()?;
            r.read_tagged_fields()?;
            parts.push((pidx, offset));
        }
        r.read_tagged_fields()?;
        layout.push((name, parts));
    }
    r.read_tagged_fields()?;

    // Stash every (topic, partition, offset) under this transaction.
    let mut per_partition_err: Vec<((String, i32), i16)> = Vec::new();
    for (topic, partitions) in &layout {
        for (pidx, offset) in partitions {
            let code = registry.stash_offset(
                &transactional_id,
                producer_id,
                producer_epoch,
                PendingOffsetCommit {
                    group_id: group_id.clone(),
                    topic: topic.clone(),
                    partition: *pidx,
                    offset: *offset,
                },
            );
            per_partition_err.push(((topic.clone(), *pidx), code));
        }
    }
    let err_map: std::collections::HashMap<(String, i32), i16> =
        per_partition_err.into_iter().collect();

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(layout.len() as u32);
    for (name, parts) in layout {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for (pidx, _offset) in parts {
            w.write_i32(pidx);
            w.write_i16(
                err_map
                    .get(&(name.clone(), pidx))
                    .copied()
                    .unwrap_or(error_codes::NONE),
            );
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lifecycle() {
        let r = TxnRegistry::default();
        let (pid1, epoch1) = r.init_producer(Some("my-txn"), 60_000);
        let (pid2, epoch2) = r.init_producer(Some("my-txn"), 60_000);
        assert_eq!(pid1, pid2, "same txn_id should reuse producer id");
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 1, "reinitialising bumps the epoch");

        let codes = r.add_partitions("my-txn", pid1, epoch2, "t", &[0, 1]);
        assert_eq!(codes, vec![0, 0]);
        let code = r.end_txn("my-txn", pid1, epoch2, true);
        assert_eq!(code, 0);
    }

    #[test]
    fn fenced_producer_rejected() {
        let r = TxnRegistry::default();
        let (pid, epoch) = r.init_producer(Some("t"), 60_000);
        let _ = r.init_producer(Some("t"), 60_000); // bumps epoch
        let codes = r.add_partitions("t", pid, epoch, "topic", &[0]);
        // Old epoch is fenced → INVALID_REQUEST per partition.
        assert!(codes.iter().all(|c| *c != error_codes::NONE));
    }

    #[test]
    fn sweep_aborts_expired_open_txns() {
        let r = TxnRegistry::default();
        // 1ms timeout — practically already expired after init.
        let (pid, epoch) = r.init_producer(Some("dead"), 1);
        let codes = r.add_partitions("dead", pid, epoch, "t", &[0]);
        assert_eq!(codes, vec![0]);
        r.record_in_flight("t", 0, pid, 0, 0);
        assert_eq!(r.last_stable_offset("t", 0), Some(0));

        // Wait past the timeout (clamped to ≥1s by init_producer, so this
        // test is slow but real).
        std::thread::sleep(std::time::Duration::from_millis(1_100));
        let aborted = r.sweep_expired();
        assert_eq!(aborted, 1);
        // After abort: in-flight cleared, range moved to aborted.
        assert_eq!(r.last_stable_offset("t", 0), None);
        let entries = r.aborted_in_range("t", 0, 0, 100);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].producer_id, pid);
    }
}
