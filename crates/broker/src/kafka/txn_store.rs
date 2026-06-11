//! Persistent backing for `TxnRegistry`. Three redb tables:
//!
//! - `producers`: `transactional_id` → encoded `(producer_id, epoch,
//!   timeout_ms, pending_offsets)`. Reloaded on broker open so a restart
//!   mid-transaction recovers the fencing state.
//! - `in_flight`: `key_be(topic, partition, producer_id, first_offset)`
//!   → `last_offset`. Reloaded so READ_COMMITTED consumers still hide
//!   uncommitted records after restart.
//! - `aborted`: `key_be(topic, partition, idx)` → `(producer_id,
//!   first_offset)`. Reloaded so the `aborted_transactions` Fetch field
//!   keeps surfacing them.
//!
//! No garbage collection yet — aborted entries accumulate forever.
//! Plans for a follow-up: drop entries below the per-partition log start
//! offset once compaction is wired up.

use std::path::Path;
use std::sync::Mutex;

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::{BrokerError, Result};

const PRODUCERS: TableDefinition<&str, &[u8]> = TableDefinition::new("txn_producers");
const IN_FLIGHT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("txn_in_flight");
const ABORTED: TableDefinition<&[u8], &[u8]> = TableDefinition::new("txn_aborted");
const COUNTERS: TableDefinition<&str, i64> = TableDefinition::new("txn_counters");

const NEXT_ABORTED_IDX: &str = "next_aborted_idx";

#[derive(Debug, Clone)]
pub struct PersistedProducer {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub timeout_ms: u64,
    pub pending_offsets: Vec<PersistedOffsetCommit>,
}

#[derive(Debug, Clone)]
pub struct PersistedOffsetCommit {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistedInFlight {
    pub producer_id: i64,
    pub first_offset: u64,
    pub last_offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PersistedAborted {
    pub producer_id: i64,
    pub first_offset: i64,
}

pub struct TxnStore {
    db: Database,
    /// Counter for the next per-aborted-entry index. Aborted entries are
    /// append-only, indexed under `(topic, partition, idx)`.
    next_aborted_idx: Mutex<i64>,
}

impl std::fmt::Debug for TxnStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnStore")
            .field("next_aborted_idx", &self.next_aborted_idx)
            .finish()
    }
}

impl TxnStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let db = Database::create(dir.join("txn_registry.redb")).map_err(map_err)?;
        let txn = db.begin_write().map_err(map_err)?;
        {
            txn.open_table(PRODUCERS).map_err(map_err)?;
            txn.open_table(IN_FLIGHT).map_err(map_err)?;
            txn.open_table(ABORTED).map_err(map_err)?;
            txn.open_table(COUNTERS).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;

        // Warm aborted counter.
        let next_aborted_idx = {
            let read = db.begin_read().map_err(map_err)?;
            let t = read.open_table(COUNTERS).map_err(map_err)?;
            t.get(NEXT_ABORTED_IDX).map_err(map_err)?.map(|g| g.value()).unwrap_or(0)
        };
        Ok(Self {
            db,
            next_aborted_idx: Mutex::new(next_aborted_idx),
        })
    }

    // ---- producers ----

    pub fn put_producer(&self, transactional_id: &str, prod: &PersistedProducer) -> Result<()> {
        let bytes = encode_producer(prod);
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut t = txn.open_table(PRODUCERS).map_err(map_err)?;
            t.insert(transactional_id, bytes.as_slice()).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    pub fn all_producers(&self) -> Result<Vec<(String, PersistedProducer)>> {
        let read = self.db.begin_read().map_err(map_err)?;
        let t = read.open_table(PRODUCERS).map_err(map_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(map_err)? {
            let (k, v) = entry.map_err(map_err)?;
            if let Some(prod) = decode_producer(v.value()) {
                out.push((k.value().to_string(), prod));
            }
        }
        Ok(out)
    }

    // ---- in-flight ranges ----

    pub fn put_in_flight(
        &self,
        topic: &str,
        partition: i32,
        range: PersistedInFlight,
    ) -> Result<()> {
        let key = encode_in_flight_key(topic, partition, range.producer_id, range.first_offset);
        let value = range.last_offset.to_be_bytes();
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut t = txn.open_table(IN_FLIGHT).map_err(map_err)?;
            t.insert(key.as_slice(), value.as_slice()).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    pub fn delete_in_flight_for_producer(
        &self,
        producer_id: i64,
        moved: &[((String, i32), PersistedInFlight)],
    ) -> Result<()> {
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut t = txn.open_table(IN_FLIGHT).map_err(map_err)?;
            for ((topic, partition), range) in moved {
                let key =
                    encode_in_flight_key(topic, *partition, range.producer_id, range.first_offset);
                let _ = t.remove(key.as_slice()).map_err(map_err)?;
            }
            let _ = producer_id; // present for caller clarity
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    pub fn all_in_flight(&self) -> Result<Vec<((String, i32), PersistedInFlight)>> {
        let read = self.db.begin_read().map_err(map_err)?;
        let t = read.open_table(IN_FLIGHT).map_err(map_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(map_err)? {
            let (k, v) = entry.map_err(map_err)?;
            let (topic, partition, producer_id, first_offset) = decode_in_flight_key(k.value())?;
            let last_offset_bytes: [u8; 8] =
                v.value().try_into().map_err(|_| corrupt())?;
            let last_offset = u64::from_be_bytes(last_offset_bytes);
            out.push((
                (topic, partition),
                PersistedInFlight {
                    producer_id,
                    first_offset,
                    last_offset,
                },
            ));
        }
        Ok(out)
    }

    // ---- aborted ranges ----

    pub fn put_aborted(
        &self,
        topic: &str,
        partition: i32,
        aborted: PersistedAborted,
    ) -> Result<()> {
        let mut idx_guard = self.next_aborted_idx.lock().unwrap();
        let idx = *idx_guard;
        *idx_guard += 1;
        drop(idx_guard);
        let key = encode_aborted_key(topic, partition, idx);
        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(&aborted.producer_id.to_be_bytes());
        value.extend_from_slice(&aborted.first_offset.to_be_bytes());

        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut t = txn.open_table(ABORTED).map_err(map_err)?;
            t.insert(key.as_slice(), value.as_slice()).map_err(map_err)?;
            let mut c = txn.open_table(COUNTERS).map_err(map_err)?;
            c.insert(NEXT_ABORTED_IDX, idx + 1).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    pub fn all_aborted(&self) -> Result<Vec<((String, i32), PersistedAborted)>> {
        let read = self.db.begin_read().map_err(map_err)?;
        let t = read.open_table(ABORTED).map_err(map_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(map_err)? {
            let (k, v) = entry.map_err(map_err)?;
            let (topic, partition, _idx) = decode_aborted_key(k.value())?;
            let bytes = v.value();
            if bytes.len() != 16 {
                continue;
            }
            let producer_id =
                i64::from_be_bytes(bytes[0..8].try_into().map_err(|_| corrupt())?);
            let first_offset =
                i64::from_be_bytes(bytes[8..16].try_into().map_err(|_| corrupt())?);
            out.push((
                (topic, partition),
                PersistedAborted {
                    producer_id,
                    first_offset,
                },
            ));
        }
        Ok(out)
    }
}

// ---- encoding ----

fn encode_producer(p: &PersistedProducer) -> Vec<u8> {
    let mut v = Vec::with_capacity(64);
    v.extend_from_slice(&p.producer_id.to_be_bytes());
    v.extend_from_slice(&p.producer_epoch.to_be_bytes());
    v.extend_from_slice(&p.timeout_ms.to_be_bytes());
    v.extend_from_slice(&(p.pending_offsets.len() as u32).to_be_bytes());
    for off in &p.pending_offsets {
        v.extend_from_slice(&(off.group_id.len() as u32).to_be_bytes());
        v.extend_from_slice(off.group_id.as_bytes());
        v.extend_from_slice(&(off.topic.len() as u32).to_be_bytes());
        v.extend_from_slice(off.topic.as_bytes());
        v.extend_from_slice(&off.partition.to_be_bytes());
        v.extend_from_slice(&off.offset.to_be_bytes());
    }
    v
}

fn decode_producer(bytes: &[u8]) -> Option<PersistedProducer> {
    if bytes.len() < 8 + 2 + 8 + 4 {
        return None;
    }
    let producer_id = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let producer_epoch = i16::from_be_bytes(bytes[8..10].try_into().ok()?);
    let timeout_ms = u64::from_be_bytes(bytes[10..18].try_into().ok()?);
    let pending_count = u32::from_be_bytes(bytes[18..22].try_into().ok()?) as usize;
    let mut c = 22usize;
    let mut pending_offsets = Vec::with_capacity(pending_count);
    for _ in 0..pending_count {
        let gl = u32::from_be_bytes(bytes[c..c + 4].try_into().ok()?) as usize;
        c += 4;
        let group_id = std::str::from_utf8(&bytes[c..c + gl]).ok()?.to_string();
        c += gl;
        let tl = u32::from_be_bytes(bytes[c..c + 4].try_into().ok()?) as usize;
        c += 4;
        let topic = std::str::from_utf8(&bytes[c..c + tl]).ok()?.to_string();
        c += tl;
        let partition = i32::from_be_bytes(bytes[c..c + 4].try_into().ok()?);
        c += 4;
        let offset = i64::from_be_bytes(bytes[c..c + 8].try_into().ok()?);
        c += 8;
        pending_offsets.push(PersistedOffsetCommit {
            group_id,
            topic,
            partition,
            offset,
        });
    }
    Some(PersistedProducer {
        producer_id,
        producer_epoch,
        timeout_ms,
        pending_offsets,
    })
}

fn encode_in_flight_key(topic: &str, partition: i32, producer_id: i64, first_offset: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 4 + 4 + 8 + 8);
    k.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    k.extend_from_slice(topic.as_bytes());
    k.extend_from_slice(&partition.to_be_bytes());
    k.extend_from_slice(&producer_id.to_be_bytes());
    k.extend_from_slice(&first_offset.to_be_bytes());
    k
}

fn decode_in_flight_key(bytes: &[u8]) -> Result<(String, i32, i64, u64)> {
    if bytes.len() < 4 {
        return Err(corrupt());
    }
    let tl = u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| corrupt())?) as usize;
    if bytes.len() != 4 + tl + 4 + 8 + 8 {
        return Err(corrupt());
    }
    let topic = std::str::from_utf8(&bytes[4..4 + tl]).map_err(|_| corrupt())?.to_string();
    let mut c = 4 + tl;
    let partition = i32::from_be_bytes(bytes[c..c + 4].try_into().map_err(|_| corrupt())?);
    c += 4;
    let producer_id = i64::from_be_bytes(bytes[c..c + 8].try_into().map_err(|_| corrupt())?);
    c += 8;
    let first_offset = u64::from_be_bytes(bytes[c..c + 8].try_into().map_err(|_| corrupt())?);
    Ok((topic, partition, producer_id, first_offset))
}

fn encode_aborted_key(topic: &str, partition: i32, idx: i64) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 4 + 4 + 8);
    k.extend_from_slice(&(topic.len() as u32).to_be_bytes());
    k.extend_from_slice(topic.as_bytes());
    k.extend_from_slice(&partition.to_be_bytes());
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

fn decode_aborted_key(bytes: &[u8]) -> Result<(String, i32, i64)> {
    if bytes.len() < 4 {
        return Err(corrupt());
    }
    let tl = u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| corrupt())?) as usize;
    if bytes.len() != 4 + tl + 4 + 8 {
        return Err(corrupt());
    }
    let topic = std::str::from_utf8(&bytes[4..4 + tl]).map_err(|_| corrupt())?.to_string();
    let mut c = 4 + tl;
    let partition = i32::from_be_bytes(bytes[c..c + 4].try_into().map_err(|_| corrupt())?);
    c += 4;
    let idx = i64::from_be_bytes(bytes[c..c + 8].try_into().map_err(|_| corrupt())?);
    Ok((topic, partition, idx))
}

fn corrupt() -> BrokerError {
    BrokerError::Custom("corrupt txn_registry redb entry".into())
}

fn map_err<E: std::fmt::Display>(e: E) -> BrokerError {
    BrokerError::Custom(format!("txn_registry redb: {e}"))
}
