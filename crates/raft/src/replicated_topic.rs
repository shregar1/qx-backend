//! `ReplicatedTopic` — a Topic-style API where every send goes through Raft,
//! with optional **transactional sends**, **isolation-level reads**, and
//! **deterministic retention** (key compaction + time-based pruning, both
//! replicated as Raft commands so every replica applies the same retention
//! decision and converges).
//!
//! - `send(key, payload)` — non-transactional write, immediately visible.
//! - `send_txn(txn_id, key, payload)` — provisional write tagged with `txn_id`.
//!   The record is visible only to `IsolationLevel::ReadUncommitted` until
//!   `commit_txn(txn_id)` flips it to Committed (visible to ReadCommitted too)
//!   or `abort_txn(txn_id)` flips it to Aborted (never visible).
//! - `begin_txn` + `*_txn_fenced` — producer-epoch-fenced transactional ops
//!   that zombie producers cannot subvert.
//! - `stream_linearizable` — leader-routed read confirming up-to-date state.
//! - `compact_by_key(partition)` — keep only the latest eligible record per
//!   key in that partition. Pending records are preserved.
//! - `compact_before(partition, cutoff_nanos)` — drop eligible records whose
//!   `created_nanos` is below the cutoff.
//! - `gc_aborted()` — local memory reclamation of aborted records.
//!
//! Per-partition counters yield deterministic record ids across replicas.
//! `created_nanos` is set once on the proposer and travels through the Raft
//! log unchanged, so every replica records the same timestamp. Compaction is
//! also a Raft command, so cutoff and key-set decisions are identical on
//! every replica.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use std::path::Path;

use crate::cluster::{Cluster, PersistentCluster};
use crate::error::{RaftError, Result};
use crate::store::ApplyCallback;
use crate::types::NodeId;

const PARTITION_HASH_SEED: u64 = 0xC0FF_EEC0_FFEE_C0FF_u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// See only non-transactional + Committed records. The default.
    ReadCommitted,
    /// Also see Pending (in-progress transaction) records. Aborted records
    /// are still hidden.
    ReadUncommitted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Command {
    Send {
        key: Vec<u8>,
        partition: u32,
        payload: Vec<u8>,
        created_nanos: u64,
    },
    CommitOffset {
        group: String,
        partition: u32,
        offset: u64,
    },
    SendTxn {
        txn_id: String,
        key: Vec<u8>,
        partition: u32,
        payload: Vec<u8>,
        created_nanos: u64,
    },
    CommitTxn {
        txn_id: String,
    },
    AbortTxn {
        txn_id: String,
    },
    BeginTxn {
        txn_id: String,
        producer_id: String,
        epoch: u32,
    },
    SendTxnFenced {
        txn_id: String,
        producer_id: String,
        epoch: u32,
        key: Vec<u8>,
        partition: u32,
        payload: Vec<u8>,
        created_nanos: u64,
    },
    CommitTxnFenced {
        txn_id: String,
        producer_id: String,
        epoch: u32,
    },
    AbortTxnFenced {
        txn_id: String,
        producer_id: String,
        epoch: u32,
    },
    /// Replicated retention: keep only the latest eligible record per key in
    /// the partition. Pending and Aborted records are left untouched.
    CompactByKey {
        partition: u32,
    },
    /// Replicated retention: drop eligible records whose `created_nanos` is
    /// strictly less than `cutoff_nanos`. Pending and Aborted records remain.
    CompactBefore {
        partition: u32,
        cutoff_nanos: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
enum RecordStatus {
    NonTransactional,
    Pending(String),
    Committed,
    Aborted,
}

fn is_compactable(status: &RecordStatus) -> bool {
    matches!(
        status,
        RecordStatus::NonTransactional | RecordStatus::Committed
    )
}

#[derive(Debug, Clone)]
struct TopicRecord {
    id: u64,
    key: Vec<u8>,
    created_nanos: u64,
    payload: Vec<u8>,
    status: RecordStatus,
}

#[derive(Default)]
struct NodeTopicState {
    partitions: HashMap<u32, Vec<TopicRecord>>,
    next_id_per_partition: HashMap<u32, u64>,
    offsets: HashMap<(String, u32), u64>,
    /// txn_id → (producer_id, epoch). Present only for fenced transactions.
    txn_producers: HashMap<String, (String, u32)>,
}

impl NodeTopicState {
    fn apply(&mut self, cmd: Command) {
        match cmd {
            Command::Send {
                key,
                partition,
                payload,
                created_nanos,
            } => {
                self.append_record(
                    partition,
                    key,
                    payload,
                    created_nanos,
                    RecordStatus::NonTransactional,
                );
            }
            Command::SendTxn {
                txn_id,
                key,
                partition,
                payload,
                created_nanos,
            } => {
                self.append_record(
                    partition,
                    key,
                    payload,
                    created_nanos,
                    RecordStatus::Pending(txn_id),
                );
            }
            Command::CommitTxn { txn_id } => {
                self.transition_txn(&txn_id, RecordStatus::Committed);
            }
            Command::AbortTxn { txn_id } => {
                self.transition_txn(&txn_id, RecordStatus::Aborted);
            }
            Command::CommitOffset {
                group,
                partition,
                offset,
            } => {
                self.offsets.insert((group, partition), offset);
            }
            Command::BeginTxn {
                txn_id,
                producer_id,
                epoch,
            } => {
                let install = match self.txn_producers.get(&txn_id) {
                    None => true,
                    Some((_, e)) => epoch > *e,
                };
                if install {
                    if self.txn_producers.contains_key(&txn_id) {
                        self.transition_txn(&txn_id, RecordStatus::Aborted);
                    }
                    self.txn_producers.insert(txn_id, (producer_id, epoch));
                }
            }
            Command::SendTxnFenced {
                txn_id,
                producer_id,
                epoch,
                key,
                partition,
                payload,
                created_nanos,
            } => {
                if self.is_current_producer(&txn_id, &producer_id, epoch) {
                    self.append_record(
                        partition,
                        key,
                        payload,
                        created_nanos,
                        RecordStatus::Pending(txn_id),
                    );
                }
            }
            Command::CommitTxnFenced {
                txn_id,
                producer_id,
                epoch,
            } => {
                if self.is_current_producer(&txn_id, &producer_id, epoch) {
                    self.transition_txn(&txn_id, RecordStatus::Committed);
                    self.txn_producers.remove(&txn_id);
                }
            }
            Command::AbortTxnFenced {
                txn_id,
                producer_id,
                epoch,
            } => {
                if self.is_current_producer(&txn_id, &producer_id, epoch) {
                    self.transition_txn(&txn_id, RecordStatus::Aborted);
                    self.txn_producers.remove(&txn_id);
                }
            }
            Command::CompactByKey { partition } => {
                if let Some(records) = self.partitions.get_mut(&partition) {
                    // Find the latest id per key, only among eligible records.
                    let mut latest_by_key: HashMap<Vec<u8>, u64> = HashMap::new();
                    for r in records.iter() {
                        if !is_compactable(&r.status) {
                            continue;
                        }
                        latest_by_key
                            .entry(r.key.clone())
                            .and_modify(|cur| {
                                if r.id > *cur {
                                    *cur = r.id;
                                }
                            })
                            .or_insert(r.id);
                    }
                    records.retain(|r| {
                        if !is_compactable(&r.status) {
                            return true;
                        }
                        latest_by_key.get(&r.key).is_none_or(|&latest| r.id == latest)
                    });
                }
            }
            Command::CompactBefore {
                partition,
                cutoff_nanos,
            } => {
                if let Some(records) = self.partitions.get_mut(&partition) {
                    records.retain(|r| {
                        if !is_compactable(&r.status) {
                            return true;
                        }
                        r.created_nanos >= cutoff_nanos
                    });
                }
            }
        }
    }

    fn append_record(
        &mut self,
        partition: u32,
        key: Vec<u8>,
        payload: Vec<u8>,
        created_nanos: u64,
        status: RecordStatus,
    ) {
        let counter = self.next_id_per_partition.entry(partition).or_insert(0);
        let id = *counter;
        *counter += 1;
        self.partitions
            .entry(partition)
            .or_default()
            .push(TopicRecord {
                id,
                key,
                created_nanos,
                payload,
                status,
            });
    }

    fn transition_txn(&mut self, txn_id: &str, to: RecordStatus) {
        for records in self.partitions.values_mut() {
            for rec in records.iter_mut() {
                if let RecordStatus::Pending(id) = &rec.status {
                    if id == txn_id {
                        rec.status = to.clone();
                    }
                }
            }
        }
    }

    fn is_current_producer(&self, txn_id: &str, producer_id: &str, epoch: u32) -> bool {
        match self.txn_producers.get(txn_id) {
            Some((pid, e)) => pid == producer_id && *e == epoch,
            None => false,
        }
    }
}

pub struct ReplicatedTopic {
    cluster: Cluster,
    states: Vec<Arc<Mutex<NodeTopicState>>>,
    partition_count: u32,
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl ReplicatedTopic {
    pub async fn bootstrap(num_nodes: u32, partition_count: u32) -> Result<Self> {
        assert!(num_nodes > 0);
        assert!(partition_count > 0);
        let states: Vec<Arc<Mutex<NodeTopicState>>> = (0..num_nodes)
            .map(|_| Arc::new(Mutex::new(NodeTopicState::default())))
            .collect();
        let appliers: Vec<ApplyCallback> = states
            .iter()
            .map(|state| {
                let state = state.clone();
                let cb: ApplyCallback = Arc::new(move |bytes: &[u8]| {
                    let Ok(cmd) = serde_json::from_slice::<Command>(bytes) else {
                        return;
                    };
                    state.lock().unwrap().apply(cmd);
                });
                cb
            })
            .collect();
        let cluster = Cluster::bootstrap_with_appliers(num_nodes, appliers).await?;
        Ok(Self {
            cluster,
            states,
            partition_count,
        })
    }

    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }

    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let h = xxh3_64_with_seed(key, PARTITION_HASH_SEED);
        (h % self.partition_count as u64) as u32
    }

    pub async fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<u32> {
        let partition = self.partition_for_key(key);
        self.propose(Command::Send {
            key: key.to_vec(),
            partition,
            payload,
            created_nanos: now_nanos(),
        })
        .await?;
        Ok(partition)
    }

    pub async fn send_txn(
        &self,
        txn_id: &str,
        key: &[u8],
        payload: Vec<u8>,
    ) -> Result<u32> {
        let partition = self.partition_for_key(key);
        self.propose(Command::SendTxn {
            txn_id: txn_id.to_string(),
            key: key.to_vec(),
            partition,
            payload,
            created_nanos: now_nanos(),
        })
        .await?;
        Ok(partition)
    }

    pub async fn commit_txn(&self, txn_id: &str) -> Result<()> {
        self.propose(Command::CommitTxn {
            txn_id: txn_id.to_string(),
        })
        .await
    }

    pub async fn abort_txn(&self, txn_id: &str) -> Result<()> {
        self.propose(Command::AbortTxn {
            txn_id: txn_id.to_string(),
        })
        .await
    }

    pub async fn begin_txn(
        &self,
        txn_id: &str,
        producer_id: &str,
        epoch: u32,
    ) -> Result<()> {
        self.propose(Command::BeginTxn {
            txn_id: txn_id.to_string(),
            producer_id: producer_id.to_string(),
            epoch,
        })
        .await
    }

    pub async fn send_txn_fenced(
        &self,
        txn_id: &str,
        producer_id: &str,
        epoch: u32,
        key: &[u8],
        payload: Vec<u8>,
    ) -> Result<u32> {
        let partition = self.partition_for_key(key);
        self.propose(Command::SendTxnFenced {
            txn_id: txn_id.to_string(),
            producer_id: producer_id.to_string(),
            epoch,
            key: key.to_vec(),
            partition,
            payload,
            created_nanos: now_nanos(),
        })
        .await?;
        Ok(partition)
    }

    pub async fn commit_txn_fenced(
        &self,
        txn_id: &str,
        producer_id: &str,
        epoch: u32,
    ) -> Result<()> {
        self.propose(Command::CommitTxnFenced {
            txn_id: txn_id.to_string(),
            producer_id: producer_id.to_string(),
            epoch,
        })
        .await
    }

    pub async fn abort_txn_fenced(
        &self,
        txn_id: &str,
        producer_id: &str,
        epoch: u32,
    ) -> Result<()> {
        self.propose(Command::AbortTxnFenced {
            txn_id: txn_id.to_string(),
            producer_id: producer_id.to_string(),
            epoch,
        })
        .await
    }

    pub async fn commit_offset(
        &self,
        group: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.propose(Command::CommitOffset {
            group: group.to_string(),
            partition,
            offset,
        })
        .await
    }

    /// Replicated key compaction: for each key in the partition, keep only
    /// the latest eligible record (non-transactional or committed). Pending
    /// records are preserved (the transaction hasn't decided yet); aborted
    /// records are also preserved (use `gc_aborted` for those).
    pub async fn compact_by_key(&self, partition: u32) -> Result<()> {
        self.propose(Command::CompactByKey { partition }).await
    }

    /// Replicated time-based pruning: drop eligible records whose
    /// `created_nanos` is strictly less than `cutoff_nanos`. The caller
    /// computes `cutoff_nanos` from a local clock — every replica sees the
    /// same value through the Raft command, so the cut is deterministic.
    pub async fn compact_before(
        &self,
        partition: u32,
        cutoff_nanos: u64,
    ) -> Result<()> {
        self.propose(Command::CompactBefore {
            partition,
            cutoff_nanos,
        })
        .await
    }

    async fn propose(&self, cmd: Command) -> Result<()> {
        let bytes = serde_json::to_vec(&cmd)
            .map_err(|e| RaftError::ClientWrite(format!("encode: {e}")))?;
        self.cluster.append(bytes).await?;
        Ok(())
    }

    pub fn stream(
        &self,
        node: NodeId,
        group: &str,
        partition: u32,
        max: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        self.stream_with_isolation(node, group, partition, max, IsolationLevel::ReadCommitted)
    }

    pub fn stream_with_isolation(
        &self,
        node: NodeId,
        group: &str,
        partition: u32,
        max: usize,
        isolation: IsolationLevel,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let state = self
            .states
            .get(node as usize)
            .ok_or(RaftError::UnknownNode(node))?
            .lock()
            .unwrap();
        let start = state
            .offsets
            .get(&(group.to_string(), partition))
            .copied()
            .unwrap_or(0);
        let empty = Vec::new();
        let records = state.partitions.get(&partition).unwrap_or(&empty);
        Ok(records
            .iter()
            .filter(|r| r.id >= start && is_visible(&r.status, isolation))
            .take(max)
            .map(|r| (r.id, r.payload.clone()))
            .collect())
    }

    pub async fn stream_linearizable(
        &self,
        group: &str,
        partition: u32,
        max: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let leader = self.cluster.leader().await.ok_or(RaftError::LeaderTimeout)?;
        self.cluster.ensure_linearizable(leader).await?;
        self.stream(leader, group, partition, max)
    }

    pub fn node_record_count(&self, node: NodeId, partition: u32) -> Result<u64> {
        let state = self
            .states
            .get(node as usize)
            .ok_or(RaftError::UnknownNode(node))?
            .lock()
            .unwrap();
        Ok(state
            .partitions
            .get(&partition)
            .map(|v| v.len() as u64)
            .unwrap_or(0))
    }

    pub async fn wait_for_apply(
        &self,
        partition: u32,
        target_count: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut all_caught_up = true;
            for n in 0..self.cluster.size() {
                if self.node_record_count(n as NodeId, partition)? < target_count {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn gc_aborted(&self) -> usize {
        let mut total = 0;
        for state in &self.states {
            let mut s = state.lock().unwrap();
            for records in s.partitions.values_mut() {
                let before = records.len();
                records.retain(|r| r.status != RecordStatus::Aborted);
                total += before - records.len();
            }
        }
        total
    }
}

fn is_visible(status: &RecordStatus, isolation: IsolationLevel) -> bool {
    match (status, isolation) {
        (RecordStatus::NonTransactional, _) => true,
        (RecordStatus::Committed, _) => true,
        (RecordStatus::Pending(_), IsolationLevel::ReadUncommitted) => true,
        (RecordStatus::Pending(_), IsolationLevel::ReadCommitted) => false,
        (RecordStatus::Aborted, _) => false,
    }
}

fn make_appliers(states: &[Arc<Mutex<NodeTopicState>>]) -> Vec<ApplyCallback> {
    states
        .iter()
        .map(|state| {
            let state = state.clone();
            let cb: ApplyCallback = Arc::new(move |bytes: &[u8]| {
                let Ok(cmd) = serde_json::from_slice::<Command>(bytes) else {
                    return;
                };
                state.lock().unwrap().apply(cmd);
            });
            cb
        })
        .collect()
}

/// Disk-backed `ReplicatedTopic`: same public surface as `ReplicatedTopic`,
/// but every replica's state is rebuilt from its on-disk Raft log on restart.
/// Built on top of `PersistentCluster` with the apply-callback hook.
pub struct PersistentReplicatedTopic {
    cluster: PersistentCluster,
    states: Vec<Arc<Mutex<NodeTopicState>>>,
    partition_count: u32,
}

impl PersistentReplicatedTopic {
    pub async fn bootstrap(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        partition_count: u32,
    ) -> Result<Self> {
        assert!(num_nodes > 0);
        assert!(partition_count > 0);
        let states: Vec<Arc<Mutex<NodeTopicState>>> = (0..num_nodes)
            .map(|_| Arc::new(Mutex::new(NodeTopicState::default())))
            .collect();
        let appliers = make_appliers(&states);
        let cluster =
            PersistentCluster::bootstrap_with_appliers(dir, num_nodes, appliers).await?;
        Ok(Self {
            cluster,
            states,
            partition_count,
        })
    }

    pub async fn open(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        partition_count: u32,
    ) -> Result<Self> {
        let states: Vec<Arc<Mutex<NodeTopicState>>> = (0..num_nodes)
            .map(|_| Arc::new(Mutex::new(NodeTopicState::default())))
            .collect();
        let appliers = make_appliers(&states);
        let cluster =
            PersistentCluster::open_with_appliers(dir, num_nodes, appliers).await?;
        Ok(Self {
            cluster,
            states,
            partition_count,
        })
    }

    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }

    pub fn partition_for_key(&self, key: &[u8]) -> u32 {
        let h = xxh3_64_with_seed(key, PARTITION_HASH_SEED);
        (h % self.partition_count as u64) as u32
    }

    pub async fn send(&self, key: &[u8], payload: Vec<u8>) -> Result<u32> {
        let partition = self.partition_for_key(key);
        let bytes = serde_json::to_vec(&Command::Send {
            key: key.to_vec(),
            partition,
            payload,
            created_nanos: now_nanos(),
        })
        .map_err(|e| RaftError::ClientWrite(format!("encode: {e}")))?;
        self.cluster.append(bytes).await?;
        Ok(partition)
    }

    pub fn stream(
        &self,
        node: NodeId,
        group: &str,
        partition: u32,
        max: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>> {
        let state = self
            .states
            .get(node as usize)
            .ok_or(RaftError::UnknownNode(node))?
            .lock()
            .unwrap();
        let start = state
            .offsets
            .get(&(group.to_string(), partition))
            .copied()
            .unwrap_or(0);
        let empty = Vec::new();
        let records = state.partitions.get(&partition).unwrap_or(&empty);
        Ok(records
            .iter()
            .filter(|r| r.id >= start && is_visible(&r.status, IsolationLevel::ReadCommitted))
            .take(max)
            .map(|r| (r.id, r.payload.clone()))
            .collect())
    }

    pub fn node_record_count(&self, node: NodeId, partition: u32) -> Result<u64> {
        let state = self
            .states
            .get(node as usize)
            .ok_or(RaftError::UnknownNode(node))?
            .lock()
            .unwrap();
        Ok(state
            .partitions
            .get(&partition)
            .map(|v| v.len() as u64)
            .unwrap_or(0))
    }

    pub async fn wait_for_apply(
        &self,
        partition: u32,
        target_count: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut all_caught_up = true;
            for n in 0..self.cluster.size() {
                if self.node_record_count(n as NodeId, partition)? < target_count {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        self.cluster.shutdown().await
    }

    pub async fn trigger_snapshot(&self, node: NodeId) -> Result<()> {
        self.cluster.trigger_snapshot(node).await
    }
}
