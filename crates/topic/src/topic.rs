//! Multi-partition topic over `qx-queue::Queue`. Routing is deterministic
//! xxh3-based hashing of a key → partition index, with a fallback `send_to`
//! for explicit partition selection. Reads are round-robin across partitions
//! with a fair per-partition budget.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::cold_store::ColdStore;

use qx_queue::{Queue, QueueConfig, ReceiptHandle};
use qx_storage::RecordId;

#[derive(Debug, Clone)]
pub struct TopicStreamedMessage {
    pub partition: u32,
    pub id: RecordId,
    /// Refcounted slice from the disk-read buffer (`bytes::Bytes`).
    /// Zero-copy down to qx-storage's `Segment::read` — the broker
    /// can hand this straight to prost on the gRPC reply path.
    pub payload: bytes::Bytes,
}
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::error::{Result, TopicError};

const META_FILENAME: &str = "meta.bin";
const META_MAGIC: &[u8; 4] = b"QX01";
const META_SIZE: usize = 8;
const PARTITION_HASH_SEED: u64 = 0xC0FF_EEC0_FFEE_C0FF_u64;

#[derive(Clone, Debug)]
pub struct TopicConfig {
    pub partition_count: u32,
    pub queue: QueueConfig,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            partition_count: 4,
            queue: QueueConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicReceipt {
    pub partition: u32,
    pub inner: ReceiptHandle,
}

#[derive(Debug, Clone)]
pub struct TopicMessage {
    pub partition: u32,
    pub id: RecordId,
    pub receipt: TopicReceipt,
    pub attempts: u32,
    /// Refcounted slice from the disk-read buffer (`bytes::Bytes`).
    /// Zero-copy down to qx-storage's `Segment::read` — the broker
    /// can hand this straight to prost on the gRPC reply path.
    pub payload: bytes::Bytes,
}

#[derive(Debug, Default, Clone)]
pub struct TopicStats {
    pub partition_count: u32,
    pub pending: u64,
    pub inflight: u64,
    pub dead: u64,
}

pub struct Topic {
    dir: PathBuf,
    /// Each partition is independently lockable so concurrent producers
    /// to different partitions don't serialize on a single Topic-wide
    /// mutex. `&self` methods lock only the partition they touch.
    partitions: Vec<Mutex<Queue>>,
    config: TopicConfig,
    /// Optional cold-tier store. When set, `read_record` falls through
    /// to it for records the hot tier doesn't have. Offloading from hot
    /// to cold is explicit via `offload_to_cold`.
    cold: Option<Arc<dyn ColdStore>>,
}

impl Topic {
    pub fn open(dir: impl AsRef<Path>, config: TopicConfig) -> Result<Self> {
        if config.partition_count == 0 {
            return Err(TopicError::ZeroPartitions);
        }
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        match read_meta(&dir)? {
            Some(existing) if existing != config.partition_count => {
                return Err(TopicError::PartitionCountMismatch {
                    configured: config.partition_count,
                    on_disk: existing,
                });
            }
            Some(_) => {}
            None => write_meta(&dir, config.partition_count)?,
        }

        let mut partitions = Vec::with_capacity(config.partition_count as usize);
        for p in 0..config.partition_count {
            let pdir = dir.join(format!("partition-{p}"));
            partitions.push(Mutex::new(Queue::open(&pdir, config.queue.clone())?));
        }

        Ok(Self {
            dir,
            partitions,
            config,
            cold: None,
        })
    }

    /// Attach a cold-tier store. `read_record` will fall through to it
    /// when the hot tier returns `None`. Pure addition — does not
    /// migrate any existing hot data.
    pub fn with_cold_store(mut self, store: Arc<dyn ColdStore>) -> Self {
        self.cold = Some(store);
        self
    }

    /// Offload the record at `(partition, id)` to the cold tier (if
    /// configured) without removing it from the hot tier. Intended to
    /// run inside a retention sweeper that bulk-offloads then advances
    /// a broker-level low watermark. Returns `Ok(false)` when no cold
    /// store is attached or when the hot tier doesn't have the record.
    pub fn offload_to_cold(&self, partition: u32, id: RecordId) -> Result<bool> {
        let Some(cold) = self.cold.as_ref() else {
            return Ok(false);
        };
        let idx = self.check_partition(partition)?;
        let payload = match self.partitions[idx].lock().unwrap().read_record(id) {
            Some(p) => p,
            None => return Ok(false),
        };
        cold.put(partition, id, &payload)?;
        Ok(true)
    }

    pub fn partition_count(&self) -> u32 {
        self.config.partition_count
    }

    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let h = xxh3_64_with_seed(key, PARTITION_HASH_SEED);
        (h % self.config.partition_count as u64) as u32
    }

    pub fn send(&self, key: &[u8], payload: &[u8]) -> Result<(u32, RecordId)> {
        let p = self.partition_for_key(key);
        let id = self.partitions[p as usize].lock().unwrap().send(payload)?;
        Ok((p, id))
    }

    pub fn send_to(&self, partition: u32, payload: &[u8]) -> Result<RecordId> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].lock().unwrap().send(payload)?)
    }

    /// Refcounted-buffer variant of [`Self::send_to`]. Callers that
    /// already hold a `Bytes` (outbox dispatcher, forwarding paths)
    /// pass the buffer through without an intermediate slice
    /// borrow. Internally the bytes are passed to the underlying
    /// `Queue::send` as `&[u8]`.
    pub fn send_to_bytes(
        &self,
        partition: u32,
        payload: bytes::Bytes,
    ) -> Result<RecordId> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx]
            .lock()
            .unwrap()
            .send(payload.as_ref())?)
    }

    /// Batch-append `payloads` to a single partition under one redb
    /// commit. ~N× faster than calling `send_to` N times because every
    /// per-record commit otherwise triggers an fsync. Locks only this
    /// partition — concurrent calls to different partitions don't block.
    pub fn send_batch_to(
        &self,
        partition: u32,
        payloads: &[&[u8]],
    ) -> Result<Vec<RecordId>> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].lock().unwrap().send_batch(payloads)?)
    }

    /// Apply `payloads` to `partition` under a Raft apply-cursor
    /// guard. Returns `Ok(None)` when `raft_index` is `<=` the
    /// partition's persisted cursor (duplicate apply from openraft's
    /// crash-recovery path). Atomically: the cursor write and the
    /// topic data write are in one redb transaction.
    pub fn send_batch_with_cursor(
        &self,
        partition: u32,
        payloads: &[&[u8]],
        raft_index: u64,
    ) -> Result<Option<Vec<RecordId>>> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx]
            .lock()
            .unwrap()
            .send_batch_with_cursor(payloads, raft_index)?)
    }

    pub fn send_keyed(&self, key: &[u8], payload: &[u8]) -> Result<(u32, RecordId)> {
        let p = self.partition_for_key(key);
        let id = self.partitions[p as usize].lock().unwrap().send_keyed(key, payload)?;
        Ok((p, id))
    }

    pub fn compact_by_key(&self) -> Result<usize> {
        let mut total = 0;
        for q in &self.partitions {
            total += q.lock().unwrap().compact_by_key()?;
        }
        Ok(total)
    }

    pub fn stream(
        &self,
        group: &str,
        partition: u32,
        max: usize,
    ) -> Result<Vec<TopicStreamedMessage>> {
        let idx = self.check_partition(partition)?;
        let msgs = self.partitions[idx].lock().unwrap().stream(group, max)?;
        Ok(msgs
            .into_iter()
            .map(|m| TopicStreamedMessage {
                partition,
                id: m.id,
                payload: m.payload,
            })
            .collect())
    }

    pub fn commit_offset(
        &self,
        group: &str,
        partition: u32,
        offset: RecordId,
    ) -> Result<()> {
        let idx = self.check_partition(partition)?;
        self.partitions[idx].lock().unwrap().commit_offset(group, offset)?;
        Ok(())
    }

    pub fn group_offset(&self, group: &str, partition: u32) -> Result<Option<RecordId>> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].lock().unwrap().group_offset(group)?)
    }

    /// High-watermark: id of the next record that would be assigned in
    /// this partition. Kafka clients call this the "log end offset".
    pub fn partition_end_offset(&self, partition: u32) -> Result<RecordId> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].lock().unwrap().next_record_id())
    }

    /// Read one record by `id` from `partition`. Falls through to the
    /// cold tier (if attached) when the hot tier doesn't have it —
    /// either because the id is past the end of the hot log or because
    /// retention truncated it.
    pub fn read_record(&self, partition: u32, id: RecordId) -> Result<Option<bytes::Bytes>> {
        let idx = self.check_partition(partition)?;
        let hot = self.partitions[idx].lock().unwrap().read_record(id);
        if hot.is_some() {
            return Ok(hot);
        }
        if let Some(cold) = self.cold.as_ref() {
            // Cold tier still returns `Vec<u8>` (it's a separate
            // file-system backend); wrap it via the same zero-cost
            // move trick.
            return Ok(cold.get(partition, id)?.map(bytes::Bytes::from));
        }
        Ok(None)
    }

    pub fn send_batch(&self, items: &[(&[u8], &[u8])]) -> Result<Vec<(u32, RecordId)>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.config.partition_count as usize;
        let mut indices_by_partition: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, (key, _)) in items.iter().enumerate() {
            let p = self.partition_for_key(key) as usize;
            indices_by_partition[p].push(i);
        }

        let mut results = vec![(0u32, 0u64); items.len()];
        for (p_idx, indices) in indices_by_partition.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let payloads: Vec<&[u8]> = indices.iter().map(|i| items[*i].1).collect();
            let ids = self.partitions[p_idx].lock().unwrap().send_batch(&payloads)?;
            for (i, id) in indices.iter().zip(ids.iter()) {
                results[*i] = (p_idx as u32, *id);
            }
        }
        Ok(results)
    }

    pub fn receive(&self, max: usize) -> Result<Vec<TopicMessage>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let n = self.partitions.len();
        let per_partition = max.div_ceil(n);
        let mut out = Vec::with_capacity(max);
        for p_idx in 0..n {
            if out.len() >= max {
                break;
            }
            let need = (max - out.len()).min(per_partition);
            let msgs = self.partitions[p_idx].lock().unwrap().receive(need)?;
            for m in msgs {
                out.push(wrap_message(p_idx as u32, m));
            }
        }
        Ok(out)
    }

    pub fn receive_from(&self, partition: u32, max: usize) -> Result<Vec<TopicMessage>> {
        let idx = self.check_partition(partition)?;
        let msgs = self.partitions[idx].lock().unwrap().receive(max)?;
        Ok(msgs.into_iter().map(|m| wrap_message(partition, m)).collect())
    }

    pub fn ack(&self, receipt: TopicReceipt) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].lock().unwrap().ack(receipt.inner)?;
        Ok(())
    }

    pub fn nack(&self, receipt: TopicReceipt) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].lock().unwrap().nack(receipt.inner)?;
        Ok(())
    }

    pub fn extend_visibility(&self, receipt: TopicReceipt, ext: Duration) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].lock().unwrap().extend_visibility(receipt.inner, ext)?;
        Ok(())
    }

    pub fn stats(&self) -> Result<TopicStats> {
        let mut stats = TopicStats {
            partition_count: self.config.partition_count,
            ..Default::default()
        };
        for q in &self.partitions {
            let s = q.lock().unwrap().stats()?;
            stats.pending += s.pending;
            stats.inflight += s.inflight;
            stats.dead += s.dead;
        }
        Ok(stats)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn check_partition(&self, p: u32) -> Result<usize> {
        if p >= self.config.partition_count {
            return Err(TopicError::InvalidPartition {
                partition: p,
                count: self.config.partition_count,
            });
        }
        Ok(p as usize)
    }
}

fn wrap_message(partition: u32, msg: qx_queue::Message) -> TopicMessage {
    TopicMessage {
        partition,
        id: msg.id,
        receipt: TopicReceipt {
            partition,
            inner: msg.receipt,
        },
        attempts: msg.attempts,
        payload: msg.payload,
    }
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join(META_FILENAME)
}

/// Public: peek at the partition count an existing topic dir was opened
/// with, without actually opening the partition logs. Used by callers
/// that need to discover topics on disk at startup (e.g. broker recovery).
pub fn read_partition_count(dir: impl AsRef<Path>) -> Result<Option<u32>> {
    read_meta(dir.as_ref())
}

fn write_meta(dir: &Path, partition_count: u32) -> Result<()> {
    let mut buf = [0u8; META_SIZE];
    buf[0..4].copy_from_slice(&partition_count.to_le_bytes());
    buf[4..8].copy_from_slice(META_MAGIC);
    std::fs::write(meta_path(dir), buf)?;
    Ok(())
}

fn read_meta(dir: &Path) -> Result<Option<u32>> {
    let path = meta_path(dir);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if bytes.len() != META_SIZE || &bytes[4..8] != META_MAGIC {
        return Err(TopicError::CorruptMetadata);
    }
    Ok(Some(u32::from_le_bytes(bytes[0..4].try_into().unwrap())))
}
