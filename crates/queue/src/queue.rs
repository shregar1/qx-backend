//! Queue API: per-message lifecycle (Pending → InFlight → Acked/Dead)
//! layered over a durable record log.
//!
//! Crash-safety:
//! - The log is fsync'd on send when `durable_send` is set; redb commits its
//!   txn (and fsyncs) before each public method returns.
//! - On open, log records past the max id known to state are re-introduced as
//!   Pending. This covers the window between `log.append` and `state.commit`.

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qx_storage::{PartitionConfig, PartitionLog, RecordId};
use redb::{Database, ReadableTable, TableDefinition};

use crate::error::{QueueError, Result};
use crate::state::{MessageState, ReceiptHandle, Status};

const MESSAGES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("messages");
const DEDUP_TABLE: TableDefinition<&[u8], u64> = TableDefinition::new("dedup");
const OFFSETS_TABLE: TableDefinition<&str, u64> = TableDefinition::new("offsets");

pub const DEFAULT_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

#[derive(Clone, Debug)]
pub struct QueueConfig {
    pub partition: PartitionConfig,
    pub visibility_timeout: Duration,
    pub max_attempts: u32,
    pub durable_send: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            partition: PartitionConfig::default(),
            visibility_timeout: DEFAULT_VISIBILITY_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            durable_send: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: RecordId,
    pub receipt: ReceiptHandle,
    pub attempts: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StreamedMessage {
    pub id: RecordId,
    pub payload: Vec<u8>,
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
    pub fn open(dir: impl AsRef<Path>, config: QueueConfig) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let log = PartitionLog::open(dir.join("log"), config.partition.clone())?;
        let state_db = Database::create(dir.join("state.redb"))?;

        ensure_schema(&state_db)?;

        let max_known = read_max_known(&state_db)?;
        let log_next = log.next_record_id();
        let recovery_start = max_known.map(|m| m + 1).unwrap_or(0);
        if recovery_start < log_next {
            let write_txn = state_db.begin_write()?;
            {
                let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
                for id in recovery_start..log_next {
                    let bytes = MessageState::pending().encode();
                    messages.insert(id, bytes.as_slice())?;
                }
            }
            write_txn.commit()?;
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
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let bytes = MessageState::pending().encode();
            messages.insert(id, bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(id)
    }

    pub fn send_keyed(&mut self, key: &[u8], payload: &[u8]) -> Result<RecordId> {
        let id = self.log.append(payload)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let state = MessageState::pending_with_compaction_key(key.to_vec());
            messages.insert(id, state.encode().as_slice())?;
        }
        write_txn.commit()?;
        Ok(id)
    }

    pub fn compact_by_key(&mut self) -> Result<usize> {
        use std::collections::HashMap;
        let write_txn = self.state_db.begin_write()?;
        let mut compacted = 0;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let mut latest_per_key: HashMap<Vec<u8>, RecordId> = HashMap::new();
            let mut to_remove: Vec<RecordId> = Vec::new();
            {
                let iter = messages.iter()?;
                for entry in iter {
                    let (id_g, state_g) = entry?;
                    let id = id_g.value();
                    let state = MessageState::decode(state_g.value())?;
                    let Some(key) = state.compaction_key.clone() else {
                        continue;
                    };
                    if state.status != Status::Pending {
                        continue;
                    }
                    if let Some(prev) = latest_per_key.insert(key, id) {
                        to_remove.push(prev);
                    }
                }
            }
            for id in to_remove {
                messages.remove(id)?;
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
        let write_txn = self.state_db.begin_write()?;
        let final_id = {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
            if let Some(existing) = dedup.get(key)? {
                existing.value()
            } else {
                let state = MessageState::pending_with_dedup(key.to_vec());
                messages.insert(id, state.encode().as_slice())?;
                dedup.insert(key, id)?;
                id
            }
        };
        write_txn.commit()?;
        Ok((final_id, final_id != id))
    }

    pub fn send_batch(&mut self, payloads: &[&[u8]]) -> Result<Vec<RecordId>> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let ids = self.log.append_batch(payloads)?;
        if self.config.durable_send {
            self.log.sync()?;
        }
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let bytes = MessageState::pending().encode();
            for id in &ids {
                messages.insert(*id, bytes.as_slice())?;
            }
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

        let write_txn = self.state_db.begin_write()?;
        let mut delivered = Vec::new();
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;

            let candidates: Vec<(RecordId, MessageState)> = {
                let iter = messages.iter()?;
                let mut acc = Vec::new();
                for entry in iter {
                    if acc.len() >= max {
                        break;
                    }
                    let (id_g, state_g) = entry?;
                    let state = MessageState::decode(state_g.value())?;
                    if state.status == Status::Dead {
                        continue;
                    }
                    if !state.is_claimable(now) {
                        continue;
                    }
                    acc.push((id_g.value(), state));
                }
                acc
            };

            for (id, mut state) in candidates {
                state.attempts = state.attempts.saturating_add(1);
                if state.attempts > max_attempts {
                    state.status = Status::Dead;
                    state.inflight_until_nanos = 0;
                    messages.insert(id, state.encode().as_slice())?;
                    continue;
                }
                state.status = Status::InFlight;
                state.inflight_until_nanos = now.saturating_add(visibility_nanos);
                let receipt = ReceiptHandle {
                    record_id: id,
                    attempt: state.attempts,
                };
                let payload = self.log.read(id)?.into_payload();
                messages.insert(id, state.encode().as_slice())?;
                delivered.push(Message {
                    id,
                    receipt,
                    attempts: state.attempts,
                    payload,
                });
            }
        }
        write_txn.commit()?;
        Ok(delivered)
    }

    pub fn ack(&mut self, receipt: ReceiptHandle) -> Result<()> {
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
            let state = load_state(&messages, receipt.record_id)?;
            check_receipt(&state, &receipt)?;
            messages.remove(receipt.record_id)?;
            if let Some(key) = state.dedup_key.as_ref() {
                dedup.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn nack(&mut self, receipt: ReceiptHandle) -> Result<()> {
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let mut state = load_state(&messages, receipt.record_id)?;
            check_receipt(&state, &receipt)?;
            state.status = Status::Pending;
            state.inflight_until_nanos = 0;
            messages.insert(receipt.record_id, state.encode().as_slice())?;
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
        let write_txn = self.state_db.begin_write()?;
        {
            let mut messages = write_txn.open_table(MESSAGES_TABLE)?;
            let mut state = load_state(&messages, receipt.record_id)?;
            check_receipt(&state, &receipt)?;
            state.inflight_until_nanos = now.saturating_add(extension.as_nanos() as u64);
            messages.insert(receipt.record_id, state.encode().as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn compact(&mut self) -> Result<usize> {
        let low_water = {
            let read_txn = self.state_db.begin_read()?;
            let messages = read_txn.open_table(MESSAGES_TABLE)?;
            match messages.first()? {
                Some((k, _)) => k.value(),
                None => self.log.next_record_id(),
            }
        };
        Ok(self.log.truncate_before(low_water)?)
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
        let write_txn = self.state_db.begin_write()?;
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
        let messages = read_txn.open_table(MESSAGES_TABLE)?;
        let mut stats = QueueStats::default();
        for entry in messages.iter()? {
            let (_, state_g) = entry?;
            let state = MessageState::decode(state_g.value())?;
            match state.status {
                Status::Pending => stats.pending += 1,
                Status::InFlight if state.inflight_until_nanos <= now => stats.pending += 1,
                Status::InFlight => stats.inflight += 1,
                Status::Dead => stats.dead += 1,
            }
        }
        Ok(stats)
    }
}

fn load_state(
    messages: &redb::Table<'_, u64, &'static [u8]>,
    record_id: RecordId,
) -> Result<MessageState> {
    let guard = messages
        .get(record_id)?
        .ok_or(QueueError::UnknownReceipt { record_id })?;
    MessageState::decode(guard.value())
}

fn check_receipt(state: &MessageState, receipt: &ReceiptHandle) -> Result<()> {
    if state.status != Status::InFlight {
        return Err(QueueError::NotInFlight);
    }
    if state.attempts != receipt.attempt {
        return Err(QueueError::StaleReceipt);
    }
    Ok(())
}

fn ensure_schema(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    txn.open_table(MESSAGES_TABLE)?;
    txn.open_table(DEDUP_TABLE)?;
    txn.open_table(OFFSETS_TABLE)?;
    txn.commit()?;
    Ok(())
}

fn read_max_known(db: &Database) -> Result<Option<RecordId>> {
    let read_txn = db.begin_read()?;
    let messages = read_txn.open_table(MESSAGES_TABLE)?;
    let last = messages.last()?;
    Ok(last.map(|(k, _)| k.value()))
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
