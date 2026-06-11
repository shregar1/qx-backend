//! Replication seam.
//!
//! The broker's data-plane (Produce + transactional commit) talks to
//! the `Replicator` trait whenever cluster mode is active. The trait
//! has a single fan-out method — `replicate_produce` — that takes a
//! batch of payloads bound for a `(topic, partition)` and returns the
//! assigned record ids once consensus is reached. Implementations:
//!
//! - [`NoopReplicator`]: applies locally with no consensus. Used when
//!   `BrokerConfig::cluster` is `None`.
//! - [`RaftReplicator`]: wraps [`qx_raft::Cluster`]. Each
//!   `replicate_produce` call is serialised as a Raft log entry. Once
//!   Raft commits the entry (majority replication), every replica's
//!   apply callback fires and writes the same payloads to its local
//!   `qx_topic::Topic` — replicas converge because every write is
//!   applied in the same order on every replica. The proposer's apply
//!   path also signals the in-memory `pending` registry so the call
//!   can return the assigned record ids to the caller.
//!
//! The `Replicator` trait deliberately stays narrow. Fetch, ack/nack,
//! commit_offset etc. continue to read local state — Raft only orders
//! writes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use qx_raft::{ApplyCallback, NodeId, RaftCluster};
use qx_topic::{Topic, TopicError};

/// One write that needs to be replicated. Carries everything the apply
/// callback needs to deterministically reproduce the local-side effect
/// on every replica.
#[derive(Debug, Clone)]
pub struct ProduceCommand {
    pub topic: String,
    pub partition: u32,
    /// Refcounted slices into the originating Produce frame. Carrying
    /// `Bytes` here lets the local-write path (NoopReplicator) hand
    /// `&[u8]` views straight to `send_batch_to` without an alloc per
    /// payload. The Raft path converts to `Vec<Vec<u8>>` at the
    /// serialise step — Raft was already the slow path.
    pub payloads: Vec<bytes::Bytes>,
}

#[async_trait]
pub trait Replicator: Send + Sync + std::fmt::Debug {
    /// `true` when this replicator does no consensus — the local
    /// `Topic` is the only state and `replicate_produce` is a thin
    /// wrapper over `topic.send_batch_to`. The writer task uses this
    /// (together with the topic's `inline_safe` flag) to skip the
    /// `spawn_blocking` roundtrip and apply the batch directly on the
    /// tokio worker. Defaults to `false` so any new replicator opts
    /// in explicitly.
    fn is_local_only(&self) -> bool {
        false
    }

    /// Replicate the produce, wait for consensus, then return the
    /// assigned record ids in the order of `cmd.payloads`. The local
    /// apply must already have happened by the time this returns.
    async fn replicate_produce(
        &self,
        topic_handle: Arc<Topic>,
        cmd: ProduceCommand,
    ) -> Result<Vec<u64>, TopicError>;

    /// Replicate a `CreateTopic` command. After it returns Ok, every
    /// replica has been told to materialise the topic locally — the
    /// proposer's broker has done so too.
    ///
    /// Single-node (`NoopReplicator`) just no-ops — the caller is
    /// expected to have already created the topic via
    /// [`crate::Broker::open_topic_blocking`].
    async fn replicate_create_topic(
        &self,
        name: String,
        partition_count: u32,
    ) -> Result<(), TopicError>;
}

/// Pass-through replicator: writes directly to the local `Topic` with
/// no consensus. Identical to the non-replicated produce path; exists
/// so that every call site can talk to the same trait regardless of
/// whether cluster mode is enabled.
#[derive(Debug, Default)]
pub struct NoopReplicator;

#[async_trait]
impl Replicator for NoopReplicator {
    fn is_local_only(&self) -> bool {
        true
    }

    async fn replicate_create_topic(
        &self,
        _name: String,
        _partition_count: u32,
    ) -> Result<(), TopicError> {
        // No-op: single-node, no peers to inform. The caller already
        // materialised the topic via the local `Broker`.
        Ok(())
    }

    async fn replicate_produce(
        &self,
        topic_handle: Arc<Topic>,
        cmd: ProduceCommand,
    ) -> Result<Vec<u64>, TopicError> {
        // `send_batch_to` does fsync work synchronously — keep it off
        // the tokio worker. The produce_writer used to call
        // `spawn_blocking` directly; the seam moves here so all
        // callers can stay async-only.
        let payloads = cmd.payloads;
        let partition = cmd.partition;
        tokio::task::spawn_blocking(move || {
            let refs: Vec<&[u8]> = payloads.iter().map(|b| b.as_ref()).collect();
            topic_handle.send_batch_to(partition, &refs)
        })
        .await
        .map_err(|e| TopicError::Other(format!("noop replicator join: {e}")))?
    }
}

/// Serialised Raft log entry shape. Stable across versions — anything
/// added must be a new variant so old replicas can skip-and-log
/// rather than panic when they don't understand a payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ReplicatedCommand {
    Produce {
        /// Identifier assigned by the proposer's `next_request_id`. The
        /// proposer's apply callback uses this to look up the matching
        /// `oneshot::Sender` in its local `pending` registry; other
        /// replicas simply apply.
        request_id: u64,
        /// `NodeId` of the broker that called `replicate_produce` for
        /// this entry. The apply callback on every replica compares
        /// `self_node == proposer_node` to decide whether to signal a
        /// local `pending` entry or stay quiet.
        proposer_node_id: NodeId,
        /// The topic name. The applier resolves it to an `Arc<Topic>`
        /// via the broker's `TopicResolver`.
        topic: String,
        partition: u32,
        payloads: Vec<Vec<u8>>,
    },
    /// Create a topic on every replica. The proposer's apply signals
    /// `Ok(())` (empty `Vec<u64>` in the existing oneshot channel) once
    /// the local create returns successfully.
    CreateTopic {
        request_id: u64,
        proposer_node_id: NodeId,
        name: String,
        partition_count: u32,
    },
}

pub type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Vec<u64>, String>>>>>;

/// Trait used by [`RaftReplicator::applier_for_broker`] to interact
/// with the broker's topic catalogue from the apply path. Implemented
/// by the broker so the applier can both:
///   - resolve `ReplicatedCommand::Produce::topic` into an `Arc<Topic>`,
///   - materialise a brand-new topic when `ReplicatedCommand::CreateTopic`
///     arrives on every replica.
///
/// Both methods are invoked on every applied entry, so they should be
/// cheap (hashmap lookup + arc clone, or a single file-system create
/// in the topic-create case).
pub trait TopicResolver: Send + Sync + 'static {
    fn lookup(&self, name: &str) -> Option<Arc<Topic>>;

    /// Create a topic with `partition_count` partitions on this
    /// broker. Must be idempotent — calling twice with the same name
    /// and partition_count returns `Ok` both times.
    fn create_topic(
        &self,
        name: &str,
        partition_count: u32,
    ) -> std::result::Result<(), String>;
}

/// Allocate an empty pending registry. Pass the returned handle to
/// both [`RaftReplicator::applier_for`] (at cluster bootstrap) and
/// [`RaftReplicator::with_pending`] (when the replicator is
/// constructed) so the apply path and the proposer share the same
/// `request_id → oneshot` map.
pub fn fresh_pending_map() -> PendingMap {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Raft-backed replicator. The proposing broker calls
/// [`Self::replicate_produce`], which:
///
///  1. allocates a fresh `request_id`,
///  2. registers a [`tokio::sync::oneshot::Sender`] keyed by that id,
///  3. serialises a [`ReplicatedCommand::Produce`] and calls
///     [`Cluster::append`] (proposes via the current Raft leader),
///  4. awaits the oneshot — fired by *this* broker's apply callback
///     when Raft drives the entry through its local apply.
///
/// The apply callback ([`Self::applier_for`]) is registered with the
/// cluster at bootstrap. On every node it deterministically calls
/// `topic.send_batch_to(partition, payloads)` — assignments are stable
/// across nodes because every replica starts empty and processes the
/// same sequence of log entries in the same order.
pub struct RaftReplicator {
    cluster: Arc<dyn RaftCluster>,
    my_node_id: NodeId,
    pending: PendingMap,
    next_request_id: AtomicU64,
}

impl std::fmt::Debug for RaftReplicator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftReplicator")
            .field("my_node_id", &self.my_node_id)
            .field("pending_len", &self.pending.lock().unwrap().len())
            .finish()
    }
}

impl RaftReplicator {
    /// Build a replicator for the broker running as `my_node_id`
    /// inside `cluster`, sharing `pending` with the apply callback
    /// (constructed via [`Self::applier_for`] using the SAME
    /// `pending` handle). The shared map is what lets the apply
    /// callback wake the `replicate_produce` future.
    ///
    /// `cluster` is held as a `dyn` trait object so the same
    /// replicator works against the in-process
    /// [`qx_raft::Cluster`], the real-gRPC
    /// [`qx_raft::GrpcCluster`], or [`qx_raft::PersistentCluster`].
    pub fn with_pending(
        cluster: Arc<dyn RaftCluster>,
        my_node_id: NodeId,
        pending: PendingMap,
    ) -> Self {
        Self {
            cluster,
            my_node_id,
            pending,
            next_request_id: AtomicU64::new(1),
        }
    }

    pub fn pending_handle(&self) -> PendingMap {
        self.pending.clone()
    }

    /// Construct the [`ApplyCallback`] for `node_id`. Captures the
    /// node's `Topic` handle (so it can apply the writes locally) plus
    /// `pending` (so the proposer's node — and *only* the proposer's
    /// node — signals its oneshot when the apply lands).
    ///
    /// Every replica's applier should call into this — the apply
    /// itself happens on every node, but only the proposer's apply
    /// returns ids upward.
    pub fn applier_for(
        node_id: NodeId,
        topic: Arc<Topic>,
        pending: PendingMap,
    ) -> ApplyCallback {
        Arc::new(move |raft_index: u64, bytes: &[u8]| {
            let Ok(cmd) = serde_json::from_slice::<ReplicatedCommand>(bytes) else {
                // Unknown payload (or wire-format mismatch). Skipping
                // keeps replicas alive across version skew but does
                // mean a misencoded entry is silently dropped — the
                // proposer will time out on its oneshot.
                return;
            };
            match cmd {
                ReplicatedCommand::Produce {
                    request_id,
                    proposer_node_id,
                    topic: _,
                    partition,
                    payloads,
                } => {
                    let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
                    // Cursor-guarded apply: a re-apply of the same
                    // `raft_index` from openraft's crash-recovery
                    // path returns `Ok(None)` and leaves topic
                    // state untouched.
                    let result = topic.send_batch_with_cursor(partition, &refs, raft_index);
                    if proposer_node_id == node_id
                        && let Some(tx) = pending.lock().unwrap().remove(&request_id)
                    {
                        match result {
                            Ok(Some(ids)) => {
                                let _ = tx.send(Ok(ids));
                            }
                            Ok(None) => {
                                // Should be impossible for the proposer on
                                // a fresh apply — only the crash-recovery
                                // path can see a cursor-blocked None, and
                                // by then the proposer's pending entry
                                // is long gone. Surface defensively so a
                                // future bug doesn't hang clients.
                                let _ = tx.send(Err(
                                    "duplicate apply on proposer (unexpected)".into(),
                                ));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e.to_string()));
                            }
                        }
                    }
                    // Followers always apply; only the proposer signals.
                }
                ReplicatedCommand::CreateTopic { .. } => {
                    // applier_for is the single-topic test variant —
                    // it doesn't know how to materialise new topics.
                    // Brokers use `applier_for_broker` for that.
                }
            }
        })
    }

    /// Broker-aware applier. Looks up the target topic by name on
    /// every applied entry — used by `Broker::start_cluster` where
    /// the broker hosts many topics and the right one for each entry
    /// is determined by the envelope.
    pub fn applier_for_broker(
        node_id: NodeId,
        resolver: Arc<dyn TopicResolver>,
        pending: PendingMap,
    ) -> ApplyCallback {
        Arc::new(move |raft_index: u64, bytes: &[u8]| {
            let Ok(cmd) = serde_json::from_slice::<ReplicatedCommand>(bytes) else {
                return;
            };
            match cmd {
                ReplicatedCommand::Produce {
                    request_id,
                    proposer_node_id,
                    topic: name,
                    partition,
                    payloads,
                } => {
                    let topic = match resolver.lookup(&name) {
                        Some(t) => t,
                        None => {
                            if proposer_node_id == node_id
                                && let Some(tx) =
                                    pending.lock().unwrap().remove(&request_id)
                            {
                                let _ = tx.send(Err(format!(
                                    "topic '{name}' unknown on this replica"
                                )));
                            }
                            return;
                        }
                    };
                    let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
                    // Cursor-guarded apply — see `applier_for` for the
                    // detailed crash-recovery argument.
                    let result =
                        topic.send_batch_with_cursor(partition, &refs, raft_index);
                    if proposer_node_id == node_id
                        && let Some(tx) = pending.lock().unwrap().remove(&request_id)
                    {
                        match result {
                            Ok(Some(ids)) => {
                                let _ = tx.send(Ok(ids));
                            }
                            Ok(None) => {
                                let _ = tx.send(Err(
                                    "duplicate apply on proposer (unexpected)".into(),
                                ));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e.to_string()));
                            }
                        }
                    }
                }
                ReplicatedCommand::CreateTopic {
                    request_id,
                    proposer_node_id,
                    name,
                    partition_count,
                } => {
                    let result = resolver.create_topic(&name, partition_count);
                    // The proposer is the only node that needs to
                    // hear back. Followers create the topic silently.
                    if proposer_node_id == node_id
                        && let Some(tx) = pending.lock().unwrap().remove(&request_id)
                    {
                        match result {
                            Ok(()) => {
                                // The pending oneshot expects
                                // `Vec<u64>`. CreateTopic has no ids
                                // — return an empty vec. The client
                                // calls `replicate_create_topic`
                                // which discards the vec.
                                let _ = tx.send(Ok(Vec::new()));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e));
                            }
                        }
                    }
                    // `raft_index` is unused here — topic-create is
                    // idempotent at the `TopicResolver::create_topic`
                    // layer (open returns the same handle on second
                    // call) so a re-apply on restart is a no-op
                    // without an explicit cursor.
                    let _ = raft_index;
                }
            }
        })
    }
}

#[async_trait]
impl Replicator for RaftReplicator {
    async fn replicate_create_topic(
        &self,
        name: String,
        partition_count: u32,
    ) -> Result<(), TopicError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<Result<Vec<u64>, String>>();
        self.pending.lock().unwrap().insert(request_id, tx);

        let envelope = ReplicatedCommand::CreateTopic {
            request_id,
            proposer_node_id: self.my_node_id,
            name,
            partition_count,
        };
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|e| TopicError::Other(format!("serialize CreateTopic: {e}")))?;

        let append_result = self
            .cluster
            .append(bytes)
            .await
            .map_err(|e| TopicError::Other(format!("raft append: {e}")));
        if let Err(e) = &append_result {
            self.pending.lock().unwrap().remove(&request_id);
            return Err(TopicError::Other(e.to_string()));
        }
        rx.await
            .map_err(|_| TopicError::Other("replication channel closed before apply".into()))?
            .map_err(TopicError::Other)?;
        Ok(())
    }

    async fn replicate_produce(
        &self,
        _topic_handle: Arc<Topic>,
        cmd: ProduceCommand,
    ) -> Result<Vec<u64>, TopicError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel::<Result<Vec<u64>, String>>();
        self.pending.lock().unwrap().insert(request_id, tx);

        // Raft path: convert `Vec<Bytes>` → `Vec<Vec<u8>>` here for
        // serialisation. Costs a memcpy per payload but only on the
        // consensus path; the local-only bench path stays alloc-free.
        let owned_payloads: Vec<Vec<u8>> =
            cmd.payloads.iter().map(|b| b.as_ref().to_vec()).collect();
        let envelope = ReplicatedCommand::Produce {
            request_id,
            proposer_node_id: self.my_node_id,
            topic: cmd.topic,
            partition: cmd.partition,
            payloads: owned_payloads,
        };
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|e| TopicError::Other(format!("serialize replicated produce: {e}")))?;

        // `Cluster::append` finds the current leader, calls
        // `client_write` on it, waits for the entry to be committed
        // (majority replication) AND applied on the leader. After it
        // returns, our local applier has run on this node too —
        // because openraft drives apply on the leader synchronously
        // inside `client_write`, and on followers shortly afterwards.
        //
        // The pending oneshot may already be signalled by the time we
        // get back here (if this broker is the leader); otherwise we
        // await it below.
        let append_result = self
            .cluster
            .append(bytes)
            .await
            .map_err(|e| TopicError::Other(format!("raft append: {e}")));

        // If append itself failed, the apply will never fire and the
        // pending entry would leak. Remove it so we don't accumulate
        // dead senders across retries.
        if let Err(e) = &append_result {
            self.pending.lock().unwrap().remove(&request_id);
            return Err(TopicError::Other(e.to_string()));
        }

        let ids = rx
            .await
            .map_err(|_| TopicError::Other("replication channel closed before apply".into()))?
            .map_err(TopicError::Other)?;
        Ok(ids)
    }
}
