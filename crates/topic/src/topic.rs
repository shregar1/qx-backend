//! Multi-partition topic over `qx-queue::Queue`. Routing is deterministic
//! xxh3-based hashing of a key → partition index, with a fallback `send_to`
//! for explicit partition selection. Reads are round-robin across partitions
//! with a fair per-partition budget.

use std::path::{Path, PathBuf};
use std::time::Duration;

use qx_queue::{Queue, QueueConfig, ReceiptHandle};
use qx_storage::RecordId;

#[derive(Debug, Clone)]
pub struct TopicStreamedMessage {
    pub partition: u32,
    pub id: RecordId,
    pub payload: Vec<u8>,
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
    pub payload: Vec<u8>,
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
    partitions: Vec<Queue>,
    config: TopicConfig,
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
            partitions.push(Queue::open(&pdir, config.queue.clone())?);
        }

        Ok(Self {
            dir,
            partitions,
            config,
        })
    }

    pub fn partition_count(&self) -> u32 {
        self.config.partition_count
    }

    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let h = xxh3_64_with_seed(key, PARTITION_HASH_SEED);
        (h % self.config.partition_count as u64) as u32
    }

    pub fn send(&mut self, key: &[u8], payload: &[u8]) -> Result<(u32, RecordId)> {
        let p = self.partition_for_key(key);
        let id = self.partitions[p as usize].send(payload)?;
        Ok((p, id))
    }

    pub fn send_to(&mut self, partition: u32, payload: &[u8]) -> Result<RecordId> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].send(payload)?)
    }

    pub fn send_keyed(&mut self, key: &[u8], payload: &[u8]) -> Result<(u32, RecordId)> {
        let p = self.partition_for_key(key);
        let id = self.partitions[p as usize].send_keyed(key, payload)?;
        Ok((p, id))
    }

    pub fn compact_by_key(&mut self) -> Result<usize> {
        let mut total = 0;
        for q in &mut self.partitions {
            total += q.compact_by_key()?;
        }
        Ok(total)
    }

    pub fn stream(
        &mut self,
        group: &str,
        partition: u32,
        max: usize,
    ) -> Result<Vec<TopicStreamedMessage>> {
        let idx = self.check_partition(partition)?;
        let msgs = self.partitions[idx].stream(group, max)?;
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
        &mut self,
        group: &str,
        partition: u32,
        offset: RecordId,
    ) -> Result<()> {
        let idx = self.check_partition(partition)?;
        self.partitions[idx].commit_offset(group, offset)?;
        Ok(())
    }

    pub fn group_offset(&self, group: &str, partition: u32) -> Result<Option<RecordId>> {
        let idx = self.check_partition(partition)?;
        Ok(self.partitions[idx].group_offset(group)?)
    }

    pub fn send_batch(&mut self, items: &[(&[u8], &[u8])]) -> Result<Vec<(u32, RecordId)>> {
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
            let ids = self.partitions[p_idx].send_batch(&payloads)?;
            for (i, id) in indices.iter().zip(ids.iter()) {
                results[*i] = (p_idx as u32, *id);
            }
        }
        Ok(results)
    }

    pub fn receive(&mut self, max: usize) -> Result<Vec<TopicMessage>> {
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
            let msgs = self.partitions[p_idx].receive(need)?;
            for m in msgs {
                out.push(wrap_message(p_idx as u32, m));
            }
        }
        Ok(out)
    }

    pub fn receive_from(&mut self, partition: u32, max: usize) -> Result<Vec<TopicMessage>> {
        let idx = self.check_partition(partition)?;
        let msgs = self.partitions[idx].receive(max)?;
        Ok(msgs.into_iter().map(|m| wrap_message(partition, m)).collect())
    }

    pub fn ack(&mut self, receipt: TopicReceipt) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].ack(receipt.inner)?;
        Ok(())
    }

    pub fn nack(&mut self, receipt: TopicReceipt) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].nack(receipt.inner)?;
        Ok(())
    }

    pub fn extend_visibility(&mut self, receipt: TopicReceipt, ext: Duration) -> Result<()> {
        let idx = self.check_partition(receipt.partition)?;
        self.partitions[idx].extend_visibility(receipt.inner, ext)?;
        Ok(())
    }

    pub fn stats(&self) -> Result<TopicStats> {
        let mut stats = TopicStats {
            partition_count: self.config.partition_count,
            ..Default::default()
        };
        for q in &self.partitions {
            let s = q.stats()?;
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
