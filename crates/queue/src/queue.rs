//! Queue API: per-message lifecycle (Pending → InFlight → Acked/Dead)
//! layered over a durable record log.
//!
//! Storage model:
//! - Per-message state is range-encoded in the `RANGES_TABLE` (see
//!   [`crate::ranges`]). Contiguous Pending offsets share one redb row,
//!   so `send_batch(N)` is a single B-tree write regardless of `N`.
//!   Messages with a per-row key (dedup / compaction) cannot share a
//!   range; they live as singleton rows in the same table.
//! - Dedup keys map to their record id in `DEDUP_TABLE` for the
//!   "have I seen this key before" check.
//! - Group offsets (Kafka-style consumer groups) live in `OFFSETS_TABLE`.
//!
//! Crash-safety:
//! - The log is fsync'd on send when `durable_send` is set; the redb
//!   commit (fsync unless `eventual_commits`) is the second half of the
//!   atomicity boundary.
//! - On open, log records past the highest known range end are
//!   re-introduced as a single Pending range so they get delivered.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qx_storage::{PartitionConfig, PartitionLog, RecordId};
use redb::{Database, Durability, ReadableTable, TableDefinition, WriteTransaction};

use crate::error::{QueueError, Result};
use crate::ranges::{
    self, RANGES_TABLE, RangeEntry, append_pending_run, delete_offset, find_covering,
    insert_singleton, replace_offset_with,
};
use crate::state::{ReceiptHandle, Status};

const DEDUP_TABLE: TableDefinition<&[u8], u64> = TableDefinition::new("dedup");
const OFFSETS_TABLE: TableDefinition<&str, u64> = TableDefinition::new("offsets");
/// Per-partition apply cursor. Holds the highest Raft log index ever
/// applied to this partition through [`Queue::send_batch_with_cursor`].
/// On crash recovery, openraft may re-apply log entries whose previous
/// apply landed in the topic but whose `last_applied` advance never
/// flushed to the Raft store. The cursor — co-located with the topic
/// write inside the same redb commit — turns those re-applies into
/// no-ops. Single key `"raft"` so future cursors (per-protocol) don't
/// collide.
const APPLY_CURSOR_TABLE: TableDefinition<&str, u64> = TableDefinition::new("apply_cursor");
const APPLY_CURSOR_KEY: &str = "raft";
/// Single-cell table holding `last_produced_offset` — the highest
/// `RecordId` ever surfaced into the state DB by a successful `send` /
/// `send_batch` commit. Persisted in the same redb transaction as the
/// matching range update so it is crash-consistent with the state.
///
/// Recovery reads this on open and uses `max(max_known + 1, last_produced
/// + 1)` as the cutoff above which orphan log records (records that were
/// appended but whose state commit never landed) should be re-introduced
/// as Pending. Without this tracker, fully-acked queues would replay
/// every acked message after a reopen — exactly the bug the kill -9
/// harness caught on iteration 1.
const HIGH_WATER_TABLE: TableDefinition<&str, u64> = TableDefinition::new("high_water");
const HW_PRODUCED_KEY: &str = "produced";

pub const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Clone, Debug)]
pub struct QueueConfig {
    pub partition: PartitionConfig,
    pub visibility_timeout: Duration,
    pub max_attempts: u32,
    /// Fsync the record log on every send when `true`. `false` skips
    /// the log-side fsync but redb still fsyncs its own commit unless
    /// `eventual_commits` is also set.
    pub durable_send: bool,
    /// When `true`, redb writes use `Durability::Eventual` — no fsync
    /// on commit. The OS flushes whenever it gets around to it; recent
    /// writes can be lost on power failure but **not** on a process
    /// crash (the OS page cache survives). This is the "Kafka acks=1"
    /// equivalent: sub-ms latency, bounded data-loss window.
    pub eventual_commits: bool,
    /// When `true`, every `send` / `send_batch` writes only to the
    /// append-only log — the redb `RANGES_TABLE` is skipped entirely.
    /// Drops per-Produce overhead at the cost of losing queue semantics
    /// (`receive` / `ack` / `nack` / `extend_visibility`). Records are
    /// still readable via `read_record` and `stream`.
    pub track_message_state: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            partition: PartitionConfig::default(),
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            durable_send: true,
            eventual_commits: false,
            track_message_state: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: RecordId,
    pub receipt: ReceiptHandle,
    pub attempts: u32,
    /// Refcounted slice into the disk-read buffer. The qx-storage
    /// layer reads each record into its own `Vec<u8>` and wraps it
    /// in `bytes::Bytes` via a move (no realloc, no copy). Carrying
    /// `Bytes` here means downstream layers — qx-topic, the broker's
    /// proto encoder, tonic — can refcount-share the buffer instead
    /// of memcpying it.
    pub payload: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct StreamedMessage {
    pub id: RecordId,
    pub payload: bytes::Bytes,
}

#[derive(Debug, Default, Clone)]
pub struct QueueStats {
    pub pending: u64,
    pub inflight: u64,
    pub dead: u64,
}

pub struct Queue {
    log: PartitionLog,
    state_db: Database,
    config: QueueConfig,
}

impl Queue {
    fn begin_write(&self) -> Result<WriteTransaction> {
        let mut txn = Database::begin_write(&self.state_db)?;
        if self.config.eventual_commits {
            txn.set_durability(Durability::Eventual);
        }
        Ok(txn)
    }

    pub fn open(dir: impl AsRef<Path>, config: QueueConfig) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let log = PartitionLog::open(dir.join("log"), config.partition.clone())?;
        let state_db = Database::create(dir.join("state.redb"))?;
        ensure_schema(&state_db)?;

        if config.track_message_state {
            // Recovery: orphan log records (records appended but whose
            // state commit never landed) are surfaced as Pending.
            //
            // The cutoff is `max(max_known + 1, last_produced + 1)`.
            // Both are needed:
            //
            //  - `max_known` (highest end in the range table) handles
            //    the case where some state rows survived the crash.
            //  - `last_produced` handles the case where every range
            //    was acked away before the crash — without this we'd
            //    rewind to 0 and replay every acked offset (the bug
            //    the kill -9 harness caught on iter 1).
            let max_known = read_max_known(&state_db)?;
            let last_produced = read_high_water(&state_db)?;
            let log_next = log.next_record_id();
            let recovery_start = match (max_known, last_produced) {
                (Some(m), Some(p)) => (m + 1).max(p + 1),
                (Some(m), None) => m + 1,
                (None, Some(p)) => p + 1,
                (None, None) => 0,
            };
            if recovery_start < log_next {
                let count = log_next - recovery_start;
                let write_txn = state_db.begin_write()?;
                {
                    let mut ranges = write_txn.open_table(RANGES_TABLE)?;
                    append_pending_run(&mut ranges, recovery_start, count)?;
                    let mut hw = write_txn.open_table(HIGH_WATER_TABLE)?;
                    hw.insert(HW_PRODUCED_KEY, log_next - 1)?;
                }
                write_txn.commit()?;
            }
        }

        Ok(Self {
            log,
            state_db,
            config,
        })
    }

    pub fn send(&mut self, payload: &[u8]) -> Result<RecordId> {
        let id = self.log.append(payload)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        if !self.config.track_message_state {
            return Ok(id);
        }
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            append_pending_run(&mut ranges, id, 1)?;
            bump_high_water(&write_txn, id)?;
        }
        write_txn.commit()?;
        Ok(id)
    }

    pub fn send_keyed(&mut self, key: &[u8], payload: &[u8]) -> Result<RecordId> {
        let id = self.log.append(payload)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        if !self.config.track_message_state {
            return Ok(id);
        }
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let entry = RangeEntry::singleton_pending_compaction(id, key.to_vec());
            insert_singleton(&mut ranges, id, entry)?;
            bump_high_water(&write_txn, id)?;
        }
        write_txn.commit()?;
        Ok(id)
    }

    pub fn compact_by_key(&mut self) -> Result<usize> {
        use std::collections::HashMap;
        let write_txn = self.begin_write()?;
        let mut compacted = 0;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            // Walk all singleton rows that carry a compaction key. By
            // invariant multi-offset ranges have no keys, so any row
            // with a compaction key spans exactly one offset.
            let mut latest_per_key: HashMap<Vec<u8>, RecordId> = HashMap::new();
            let mut to_remove: Vec<RecordId> = Vec::new();
            {
                let iter = ranges.iter()?;
                for entry in iter {
                    let (k_g, v_g) = entry?;
                    let start = k_g.value();
                    let decoded = RangeEntry::decode(v_g.value())?;
                    let Some(key) = decoded.compaction_key.clone() else {
                        continue;
                    };
                    if decoded.status != Status::Pending {
                        continue;
                    }
                    if let Some(prev) = latest_per_key.insert(key, start) {
                        to_remove.push(prev);
                    }
                }
            }
            for id in to_remove {
                ranges.remove(id)?;
                compacted += 1;
            }
        }
        write_txn.commit()?;
        Ok(compacted)
    }

    pub fn send_with_dedup(&mut self, key: &[u8], payload: &[u8]) -> Result<(RecordId, bool)> {
        {
            let read_txn = self.state_db.begin_read()?;
            let dedup = read_txn.open_table(DEDUP_TABLE)?;
            if let Some(existing) = dedup.get(key)? {
                return Ok((existing.value(), true));
            }
        }
        let id = self.log.append(payload)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        let write_txn = self.begin_write()?;
        let final_id = {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
            if let Some(existing) = dedup.get(key)? {
                existing.value()
            } else {
                let entry = RangeEntry::singleton_pending_dedup(id, key.to_vec());
                insert_singleton(&mut ranges, id, entry)?;
                dedup.insert(key, id)?;
                id
            }
        };
        // The log got an entry whether or not we kept the row in
        // ranges — the duplicate-dedup path leaves an orphan record
        // in the log. Either way, the highest produced log offset is
        // `id`, so update high_water to id.
        bump_high_water(&write_txn, id)?;
        write_txn.commit()?;
        Ok((final_id, final_id != id))
    }

    /// Apply `payloads` under a Raft apply-cursor guard. Returns
    /// `Ok(None)` when `raft_index` is `<=` the persisted apply
    /// cursor on this queue — meaning a previous apply for that
    /// index already landed and this call is a duplicate
    /// re-delivery from openraft's restart-recovery path. Returns
    /// `Ok(Some(ids))` after a fresh apply.
    ///
    /// The cursor write and the log+state writes share one redb
    /// transaction, so they are durably atomic together. On crash
    /// after `Ok(Some(_))` returned: either both landed or neither
    /// landed.
    ///
    /// Behaves like [`Self::send_batch`] for the apply path itself
    /// (range-encoded state, deterministic offsets). Empty
    /// `payloads` is a no-op that still advances the cursor — this
    /// matters when openraft applies a `Blank`/`Membership` entry
    /// and the broker still wants the cursor advanced.
    pub fn send_batch_with_cursor(
        &mut self,
        payloads: &[&[u8]],
        raft_index: u64,
    ) -> Result<Option<Vec<RecordId>>> {
        // Cheap pre-check on a read txn; the authoritative check
        // happens again inside the write txn so a concurrent apply
        // can't race us into a double-apply.
        {
            let read_txn = self.state_db.begin_read()?;
            let table = read_txn.open_table(APPLY_CURSOR_TABLE)?;
            if let Some(g) = table.get(APPLY_CURSOR_KEY)?
                && raft_index <= g.value()
            {
                return Ok(None);
            }
        }

        if payloads.is_empty() {
            // Just advance the cursor.
            let write_txn = self.begin_write()?;
            {
                let mut cursor = write_txn.open_table(APPLY_CURSOR_TABLE)?;
                let current = cursor.get(APPLY_CURSOR_KEY)?.map(|g| g.value()).unwrap_or(0);
                if raft_index <= current {
                    return Ok(None);
                }
                cursor.insert(APPLY_CURSOR_KEY, raft_index)?;
            }
            write_txn.commit()?;
            return Ok(Some(Vec::new()));
        }

        let ids = self.log.append_batch(payloads)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        let first = ids[0];
        let count = ids.len() as u64;
        let last = first + count - 1;

        let write_txn = self.begin_write()?;
        // Re-check inside the write txn — defends against the
        // double-applier race window between the pre-check above and
        // this point.
        let final_ids = {
            let cursor_view = write_txn.open_table(APPLY_CURSOR_TABLE)?;
            let current = cursor_view
                .get(APPLY_CURSOR_KEY)?
                .map(|g| g.value())
                .unwrap_or(0);
            if raft_index <= current {
                // Duplicate apply discovered. Abandon the txn — we
                // already appended to the log on disk, which leaks
                // `ids.len()` slots but is otherwise harmless: those
                // log entries have no state-table rows and won't be
                // surfaced through receive/stream from this queue.
                drop(cursor_view);
                return Ok(None);
            }
            drop(cursor_view);
            if self.config.track_message_state {
                let mut ranges = write_txn.open_table(RANGES_TABLE)?;
                append_pending_run(&mut ranges, first, count)?;
                bump_high_water(&write_txn, last)?;
            }
            let mut cursor = write_txn.open_table(APPLY_CURSOR_TABLE)?;
            cursor.insert(APPLY_CURSOR_KEY, raft_index)?;
            ids
        };
        write_txn.commit()?;
        Ok(Some(final_ids))
    }

    pub fn send_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<RecordId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let ids = self.log.append_batch(payloads)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        if !self.config.track_message_state {
            return Ok(ids);
        }
        let first = ids[0];
        let count = ids.len() as u64;
        let last = first + count - 1;
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            // Single B-tree write for the whole batch — the headline
            // win over the old per-message insert.
            append_pending_run(&mut ranges, first, count)?;
            bump_high_water(&write_txn, last)?;
        }
        write_txn.commit()?;
        Ok(ids)
    }

    pub fn receive(&mut self, max: usize) -> Result<Vec<Message>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let now = now_nanos();
        let visibility_nanos = self.config.visibility_timeout.as_nanos() as u64;
        let max_attempts = self.config.max_attempts;

        // Pass 1: walk ranges in offset order and decide what to claim.
        // Each entry in `plan` says "from range starting at `start`,
        // claim offsets `[start, claim_end)` at attempt count `attempts`
        // — and the range's tail beyond `claim_end` is left alone (or
        // was the whole range if claim_end == range.end)."
        let mut plan: Vec<ClaimPlan> = Vec::new();
        let mut budget = max as u64;
        {
            let read_txn = self.state_db.begin_read()?;
            let ranges = read_txn.open_table(RANGES_TABLE)?;
            let iter = ranges.iter()?;
            for entry in iter {
                if budget == 0 {
                    break;
                }
                let (k_g, v_g) = entry?;
                let start = k_g.value();
                let decoded = RangeEntry::decode(v_g.value())?;
                if !decoded.is_claimable(now) {
                    continue;
                }
                let length = decoded.length(start);
                let take = length.min(budget);
                let claim_end = start + take;
                plan.push(ClaimPlan {
                    start,
                    range_end: decoded.end_offset,
                    claim_end,
                    prior: decoded,
                });
                budget -= take;
            }
        }

        if plan.is_empty() {
            return Ok(Vec::new());
        }

        // Pass 2: apply the mutations and assemble deliveries.
        let mut delivered = Vec::new();
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            for step in plan {
                let new_attempts = step.prior.attempts.saturating_add(1);
                let exceeded = new_attempts > max_attempts;
                let claim_status = if exceeded { Status::Dead } else { Status::InFlight };
                let new_inflight_until = if exceeded {
                    0
                } else {
                    now.saturating_add(visibility_nanos)
                };

                let whole_range = step.claim_end == step.range_end;
                if whole_range {
                    // Mutate the range in place; no split.
                    let mutated = RangeEntry {
                        end_offset: step.range_end,
                        status: claim_status,
                        attempts: new_attempts,
                        inflight_until_nanos: new_inflight_until,
                        ..step.prior.clone()
                    };
                    ranges.insert(step.start, mutated.encode().as_slice())?;
                } else {
                    // Split: head [start, claim_end) becomes
                    // InFlight/Dead, tail [claim_end, range_end) stays
                    // at the prior state.
                    let head = RangeEntry {
                        end_offset: step.claim_end,
                        status: claim_status,
                        attempts: new_attempts,
                        inflight_until_nanos: new_inflight_until,
                        // Singletons with keys never split — they are
                        // whole-range claims by definition.
                        ..step.prior.clone()
                    };
                    let tail = RangeEntry {
                        end_offset: step.range_end,
                        // Multi-offset ranges have no keys; on split the
                        // tail keeps the prior state but drops keys
                        // defensively (they were already None).
                        dedup_key: None,
                        compaction_key: None,
                        ..step.prior.clone()
                    };
                    ranges.insert(step.start, head.encode().as_slice())?;
                    ranges.insert(step.claim_end, tail.encode().as_slice())?;
                }

                if exceeded {
                    // Don't deliver — the head range went to Dead.
                    continue;
                }

                for offset in step.start..step.claim_end {
                    let payload = self.log.read(offset)?.into_payload();
                    delivered.push(Message {
                        id: offset,
                        receipt: ReceiptHandle {
                            record_id: offset,
                            attempt: new_attempts,
                        },
                        attempts: new_attempts,
                        payload,
                    });
                }
            }
        }
        write_txn.commit()?;
        Ok(delivered)
    }

    pub fn ack(&mut self, receipt: ReceiptHandle) -> Result<()> {
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
            let (start, entry) = find_covering(&ranges, receipt.record_id)?
                .ok_or(QueueError::UnknownReceipt {
                    record_id: receipt.record_id,
                })?;
            check_receipt(&entry, &receipt)?;
            if let Some(key) = entry.dedup_key.as_ref() {
                dedup.remove(key.as_slice())?;
            }
            delete_offset(&mut ranges, start, &entry, receipt.record_id)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Ack many receipts in one redb commit. Equivalent to calling
    /// `ack` once per receipt but pays the commit cost once instead of
    /// N times — the main throughput knob for high-fan-in consumers.
    pub fn ack_batch(&mut self, receipts: &[ReceiptHandle]) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
            for receipt in receipts {
                let (start, entry) = find_covering(&ranges, receipt.record_id)?
                    .ok_or(QueueError::UnknownReceipt {
                        record_id: receipt.record_id,
                    })?;
                check_receipt(&entry, receipt)?;
                if let Some(key) = entry.dedup_key.as_ref() {
                    dedup.remove(key.as_slice())?;
                }
                delete_offset(&mut ranges, start, &entry, receipt.record_id)?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn nack(&mut self, receipt: ReceiptHandle) -> Result<()> {
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let (start, entry) = find_covering(&ranges, receipt.record_id)?
                .ok_or(QueueError::UnknownReceipt {
                    record_id: receipt.record_id,
                })?;
            check_receipt(&entry, &receipt)?;
            replace_offset_with(
                &mut ranges,
                start,
                &entry,
                receipt.record_id,
                Status::Pending,
                entry.attempts,
                0,
            )?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn extend_visibility(
        &mut self,
        receipt: ReceiptHandle,
        extension: Duration,
    ) -> Result<()> {
        let now = now_nanos();
        let write_txn = self.begin_write()?;
        {
            let mut ranges = write_txn.open_table(RANGES_TABLE)?;
            let (start, entry) = find_covering(&ranges, receipt.record_id)?
                .ok_or(QueueError::UnknownReceipt {
                    record_id: receipt.record_id,
                })?;
            check_receipt(&entry, &receipt)?;
            let new_deadline = now.saturating_add(extension.as_nanos() as u64);
            replace_offset_with(
                &mut ranges,
                start,
                &entry,
                receipt.record_id,
                Status::InFlight,
                entry.attempts,
                new_deadline,
            )?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<usize> {
        let low_water = {
            let read_txn = self.state_db.begin_read()?;
            let ranges = read_txn.open_table(RANGES_TABLE)?;
            match ranges::min_start_offset(&ranges)? {
                Some(o) => o,
                None => self.log.next_record_id(),
            }
        };
        Ok(self.log.truncate_before(low_water)?)
    }

    pub fn next_record_id(&self) -> RecordId {
        self.log.next_record_id()
    }

    pub fn read_record(&self, id: RecordId) -> Option<bytes::Bytes> {
        self.log.read(id).ok().map(|r| r.into_payload())
    }

    pub fn stream(&mut self, group: &str, max: usize) -> Result<Vec<StreamedMessage>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let start = {
            let read_txn = self.state_db.begin_read()?;
            let offsets = read_txn.open_table(OFFSETS_TABLE)?;
            offsets.get(group)?.map(|g| g.value()).unwrap_or(0)
        };
        let mut out = Vec::with_capacity(max.min(64));
        let mut id = start;
        let end = self.log.next_record_id();
        while out.len() < max && id < end {
            match self.log.read(id) {
                Ok(record) => out.push(StreamedMessage {
                    id,
                    payload: record.into_payload(),
                }),
                Err(_) => break,
            }
            id += 1;
        }
        Ok(out)
    }

    pub fn commit_offset(&mut self, group: &str, offset: RecordId) -> Result<()> {
        let write_txn = self.begin_write()?;
        {
            let mut offsets = write_txn.open_table(OFFSETS_TABLE)?;
            offsets.insert(group, offset)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn group_offset(&self, group: &str) -> Result<Option<RecordId>> {
        let read_txn = self.state_db.begin_read()?;
        let offsets = read_txn.open_table(OFFSETS_TABLE)?;
        Ok(offsets.get(group)?.map(|g| g.value()))
    }

    pub fn stats(&self) -> Result<QueueStats> {
        let now = now_nanos();
        let read_txn = self.state_db.begin_read()?;
        let ranges = read_txn.open_table(RANGES_TABLE)?;
        let mut stats = QueueStats::default();
        for entry in ranges.iter()? {
            let (k_g, v_g) = entry?;
            let start = k_g.value();
            let decoded = RangeEntry::decode(v_g.value())?;
            let length = decoded.length(start);
            match decoded.status {
                Status::Pending => stats.pending += length,
                Status::InFlight if decoded.inflight_until_nanos <= now => {
                    stats.pending += length
                }
                Status::InFlight => stats.inflight += length,
                Status::Dead => stats.dead += length,
            }
        }
        Ok(stats)
    }
}

struct ClaimPlan {
    start: RecordId,
    range_end: RecordId,
    claim_end: RecordId,
    prior: RangeEntry,
}

fn check_receipt(entry: &RangeEntry, receipt: &ReceiptHandle) -> Result<()> {
    if entry.status != Status::InFlight {
        return Err(QueueError::NotInFlight);
    }
    if entry.attempts != receipt.attempt {
        return Err(QueueError::StaleReceipt);
    }
    Ok(())
}

fn ensure_schema(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    txn.open_table(RANGES_TABLE)?;
    txn.open_table(DEDUP_TABLE)?;
    txn.open_table(OFFSETS_TABLE)?;
    txn.open_table(HIGH_WATER_TABLE)?;
    txn.open_table(APPLY_CURSOR_TABLE)?;
    txn.commit()?;
    Ok(())
}

fn read_max_known(db: &Database) -> Result<Option<RecordId>> {
    let read_txn = db.begin_read()?;
    let ranges = read_txn.open_table(RANGES_TABLE)?;
    ranges::max_known_offset(&ranges)
}

fn read_high_water(db: &Database) -> Result<Option<RecordId>> {
    let read_txn = db.begin_read()?;
    let hw = read_txn.open_table(HIGH_WATER_TABLE)?;
    Ok(hw.get(HW_PRODUCED_KEY)?.map(|g| g.value()))
}

/// Raise `last_produced` to `value` if it is strictly higher than the
/// current persisted high water mark. Called inside the same redb
/// transaction as the matching range update so the cell stays in sync
/// with the rest of the state.
fn bump_high_water(txn: &WriteTransaction, value: RecordId) -> Result<()> {
    let mut hw = txn.open_table(HIGH_WATER_TABLE)?;
    let current = hw.get(HW_PRODUCED_KEY)?.map(|g| g.value());
    if current.map_or(true, |c| value > c) {
        hw.insert(HW_PRODUCED_KEY, value)?;
    }
    Ok(())
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
