//! In-process N-node openraft cluster.
//!
//! `Cluster::bootstrap(n)` creates `n` Raft nodes connected via an in-process
//! `RaftNetwork`, initialises the membership, waits for leader election, and
//! exposes append/read against the resulting cluster.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::Adaptor;
use openraft::BasicNode;
use openraft::Config;
use openraft::Raft;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::time::Instant;

use crate::error::{RaftError, Result};
use crate::network::{InProcessFactory, Partitions, Registry};
use crate::persistent_store::PersistentStorage;
use crate::store::MemoryStorage;
use crate::types::{AppRequest, NodeId, TypeConfig};

pub struct ClusterNode {
    pub id: NodeId,
    pub raft: Arc<Raft<TypeConfig>>,
    pub storage: MemoryStorage,
}

pub struct Cluster {
    nodes: Vec<ClusterNode>,
    partitions: Partitions,
}

impl Cluster {
    /// Bootstrap with one `ApplyCallback` per node. Each callback fires on
    /// the corresponding node whenever an `EntryPayload::Normal` is applied,
    /// letting the caller drive external state (e.g. replicate sends into a
    /// per-replica `qx_topic::Topic`).
    pub async fn bootstrap_with_appliers(
        num_nodes: u32,
        appliers: Vec<crate::store::ApplyCallback>,
    ) -> Result<Self> {
        assert_eq!(appliers.len(), num_nodes as usize);
        Self::bootstrap_internal(num_nodes, Some(appliers)).await
    }

    pub async fn bootstrap(num_nodes: u32) -> Result<Self> {
        Self::bootstrap_internal(num_nodes, None).await
    }

    async fn bootstrap_internal(
        num_nodes: u32,
        appliers: Option<Vec<crate::store::ApplyCallback>>,
    ) -> Result<Self> {
        assert!(num_nodes > 0);
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .map_err(|e| RaftError::Initialise(e.to_string()))?,
        );

        let registry: Registry = Arc::new(RwLock::new(HashMap::new()));
        let partitions: Partitions =
            Arc::new(std::sync::RwLock::new(std::collections::HashSet::new()));

        let mut nodes = Vec::with_capacity(num_nodes as usize);
        for id in 0..(num_nodes as NodeId) {
            let storage = match &appliers {
                Some(cbs) => MemoryStorage::with_apply_callback(cbs[id as usize].clone()),
                None => MemoryStorage::new(),
            };
            let (log_store, state_machine) = Adaptor::new(storage.clone());
            let factory = InProcessFactory {
                source: id,
                registry: registry.clone(),
                partitions: partitions.clone(),
            };
            let raft = Raft::new(id, config.clone(), factory, log_store, state_machine)
                .await
                .map_err(|e| RaftError::Initialise(e.to_string()))?;
            let raft = Arc::new(raft);
            registry.write().await.insert(id, raft.clone());
            nodes.push(ClusterNode {
                id,
                raft,
                storage,
            });
        }

        // Bootstrap membership on node 0.
        let mut members = BTreeMap::new();
        for id in 0..(num_nodes as NodeId) {
            members.insert(id, BasicNode::default());
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .map_err(|e| RaftError::Initialise(e.to_string()))?;

        let cluster = Self { nodes, partitions };
        cluster.wait_for_leader(Duration::from_secs(5)).await?;
        Ok(cluster)
    }

    pub fn size(&self) -> u32 {
        self.nodes.len() as u32
    }

    pub async fn leader(&self) -> Option<NodeId> {
        for node in &self.nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if let Some(leader) = metrics.current_leader {
                return Some(leader);
            }
        }
        None
    }

    pub async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        // Multi-mode semantics:
        //   - bootstrap-all-nodes-here: pick the openraft-reported
        //     leader from `self.nodes` and call client_write.
        //   - single-node-per-process (start_node): `self.nodes` has
        //     only this broker's Raft instance. Call client_write on
        //     it; openraft surfaces ForwardToLeader through the error
        //     path when this isn't the leader. Clients retry against
        //     the real leader broker — the same pattern Kafka uses.
        let leader = self.leader().await.ok_or(RaftError::LeaderTimeout)?;
        let target = self
            .nodes
            .iter()
            .find(|n| n.id == leader)
            .or_else(|| self.nodes.first())
            .ok_or(RaftError::UnknownNode(leader))?;
        let resp = target
            .raft
            .client_write(AppRequest { payload })
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(resp.data.index)
    }

    pub async fn ensure_linearizable(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .ensure_linearizable()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    /// Simulate a network partition between `a` and `b`. Subsequent Raft RPCs
    /// in either direction will return `Unreachable` until `heal()` is called.
    pub fn partition(&self, a: NodeId, b: NodeId) {
        let mut p = self.partitions.write().expect("partitions lock poisoned");
        p.insert((a, b));
        p.insert((b, a));
    }

    pub fn heal(&self, a: NodeId, b: NodeId) {
        let mut p = self.partitions.write().expect("partitions lock poisoned");
        p.remove(&(a, b));
        p.remove(&(b, a));
    }

    pub async fn shutdown_node(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .shutdown()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    pub async fn state_log(&self, node: NodeId) -> Result<Vec<Vec<u8>>> {
        let node = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        Ok(node.storage.state_machine_log().await)
    }

    pub async fn state_len(&self, node: NodeId) -> Result<u64> {
        let node = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        Ok(node.storage.state_machine_len().await)
    }

    pub async fn wait_for_replication(
        &self,
        target_index: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut all_caught_up = true;
            for n in &self.nodes {
                if n.storage.state_machine_len().await <= target_index {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.leader().await.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }
}

/// 3+-node cluster over real tonic gRPC. Each node listens on 127.0.0.1 on
/// an OS-assigned port; messages between nodes are JSON-serialised openraft
/// RPCs ferried by the `qx-raft::grpc` service.
pub struct GrpcClusterNode {
    pub id: NodeId,
    pub raft: Arc<Raft<TypeConfig>>,
    pub storage: PersistentStorage,
}

pub struct GrpcCluster {
    nodes: Vec<GrpcClusterNode>,
}

impl GrpcCluster {
    pub async fn bootstrap(dir: impl AsRef<Path>, num_nodes: u32) -> Result<Self> {
        Self::bootstrap_inner(dir, num_nodes, None, Vec::new()).await
    }

    /// Bootstrap with TLS on the Raft inter-node transport. `server_tls`
    /// configures the per-node tonic server certificate; `client_tls`
    /// configures the CA cert + domain that every node's outgoing connections
    /// trust.
    pub async fn bootstrap_with_tls(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        server_tls: crate::grpc::RaftServerTls,
        client_tls: crate::grpc::RaftClientTls,
    ) -> Result<Self> {
        Self::bootstrap_inner(dir, num_nodes, Some((server_tls, client_tls)), Vec::new()).await
    }

    /// Bootstrap an in-process gRPC-transport cluster with one apply
    /// callback per node. Each node's `PersistentStorage` invokes its
    /// callback synchronously whenever an entry is applied to its
    /// state machine — drives per-replica external state (e.g. the
    /// broker's `Topic`). Pass an empty vec to skip callbacks.
    ///
    /// Note: the storage replays existing log entries through the
    /// callback on open. Callers whose external state is *also*
    /// persistent (like `qx_topic::Topic`) must therefore start fresh
    /// each call or accept duplicate applies. The broker integration
    /// today only bootstraps from empty.
    pub async fn bootstrap_with_appliers(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        appliers: Vec<crate::store::ApplyCallback>,
    ) -> Result<Self> {
        Self::bootstrap_inner(dir, num_nodes, None, appliers).await
    }

    async fn bootstrap_inner(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        tls: Option<(crate::grpc::RaftServerTls, crate::grpc::RaftClientTls)>,
        appliers: Vec<crate::store::ApplyCallback>,
    ) -> Result<Self> {
        assert!(num_nodes > 0);
        assert!(appliers.is_empty() || appliers.len() == num_nodes as usize);
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Bind listeners up-front so every node knows every peer's address.
        let mut listeners = Vec::with_capacity(num_nodes as usize);
        let mut addrs: HashMap<NodeId, String> = HashMap::new();
        let scheme = if tls.is_some() { "https" } else { "http" };
        for id in 0..(num_nodes as NodeId) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            addrs.insert(id, format!("{scheme}://{addr}"));
            listeners.push(listener);
        }

        let mut nodes = Vec::with_capacity(num_nodes as usize);
        for (id, listener) in listeners.into_iter().enumerate() {
            let id = id as NodeId;
            let cb = appliers.get(id as usize).cloned();
            let node = start_grpc_node_internal(
                id,
                listener,
                addrs.clone(),
                dir.join(format!("node-{id}")),
                cb,
                tls.clone(),
            )
            .await?;
            nodes.push(node);
        }

        let mut members = BTreeMap::new();
        for id in 0..(num_nodes as NodeId) {
            members.insert(id, BasicNode::default());
        }
        nodes[0]
            .raft
            .initialize(members)
            .await
            .map_err(|e| RaftError::Initialise(e.to_string()))?;

        let cluster = Self { nodes };
        cluster.wait_for_leader(Duration::from_secs(15)).await?;
        Ok(cluster)
    }

    /// Start a SINGLE Raft node belonging to a multi-process cluster.
    /// Each broker calls this with its own `node_id`, its bound TCP
    /// listener for inter-node Raft RPCs, the full set of `peer_addrs`
    /// (including itself), and an apply callback that drives its
    /// local `Topic`. Persistent Raft state goes under `dir`.
    ///
    /// The returned `GrpcCluster` contains only this one node. To
    /// bootstrap a fresh cluster on first run, ONE node (any) must
    /// also call [`Self::initialize_membership`] after every node has
    /// started. On restart, the persisted membership entry is re-read
    /// from disk and `initialize_membership` must NOT be called again.
    ///
    /// `tls = None` runs Raft RPCs in plaintext over http; `Some(...)`
    /// uses tonic-rustls on both server and client sides.
    pub async fn start_node(
        node_id: NodeId,
        listener: tokio::net::TcpListener,
        peer_addrs: HashMap<NodeId, String>,
        dir: impl AsRef<Path>,
        apply_callback: Option<crate::store::ApplyCallback>,
        tls: Option<(crate::grpc::RaftServerTls, crate::grpc::RaftClientTls)>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let node = start_grpc_node_internal(
            node_id,
            listener,
            peer_addrs,
            dir,
            apply_callback,
            tls,
        )
        .await?;
        Ok(Self { nodes: vec![node] })
    }

    /// Initialize cluster membership on this node. Call from EXACTLY
    /// ONE node in a fresh cluster after every node has started its
    /// gRPC service. On restart this is a no-op (membership lives on
    /// disk). `num_nodes` is the size of the cluster; member ids run
    /// from 0..num_nodes.
    pub async fn initialize_membership(&self, num_nodes: u32) -> Result<()> {
        assert_eq!(self.nodes.len(), 1, "initialize_membership called on multi-node GrpcCluster — only the single-node multi-process bootstrap should use this");
        let mut members = BTreeMap::new();
        for id in 0..(num_nodes as NodeId) {
            members.insert(id, BasicNode::default());
        }
        // openraft's `initialize` is idempotent: on a fresh node it
        // commits the membership entry; on a re-initialised node
        // (the membership is already in the log) it returns
        // `NotAllowed` which we treat as success.
        match self.nodes[0].raft.initialize(members).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Best-effort idempotency: openraft surfaces
                // `NotAllowed` when log isn't empty / membership is
                // already initialized. Match on the error string —
                // openraft 0.9 doesn't expose a stable error kind
                // for this path.
                let s = e.to_string();
                if s.contains("NotAllowed") || s.contains("already") {
                    Ok(())
                } else {
                    Err(RaftError::Initialise(s))
                }
            }
        }
    }

    /// Wait until any node in this single-node view knows of a Raft
    /// leader. Returns the leader's `NodeId` so the broker can decide
    /// whether to accept Produce locally (self is leader) or surface
    /// NOT_LEADER (self is a follower).
    pub async fn wait_for_known_leader(&self, timeout: Duration) -> Result<NodeId> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(l) = self.leader().await {
                return Ok(l);
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn size(&self) -> u32 {
        self.nodes.len() as u32
    }

    pub async fn leader(&self) -> Option<NodeId> {
        for node in &self.nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if let Some(leader) = metrics.current_leader {
                return Some(leader);
            }
        }
        None
    }

    pub async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        // Multi-mode semantics:
        //   - bootstrap-all-nodes-here: pick the openraft-reported
        //     leader from `self.nodes` and call client_write.
        //   - single-node-per-process (start_node): `self.nodes` has
        //     only this broker's Raft instance. Call client_write on
        //     it; openraft surfaces ForwardToLeader through the error
        //     path when this isn't the leader. Clients retry against
        //     the real leader broker — the same pattern Kafka uses.
        let leader = self.leader().await.ok_or(RaftError::LeaderTimeout)?;
        let target = self
            .nodes
            .iter()
            .find(|n| n.id == leader)
            .or_else(|| self.nodes.first())
            .ok_or(RaftError::UnknownNode(leader))?;
        let resp = target
            .raft
            .client_write(AppRequest { payload })
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(resp.data.index)
    }

    pub async fn ensure_linearizable(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .ensure_linearizable()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    pub async fn shutdown_node(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .shutdown()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    pub async fn state_log(&self, node: NodeId) -> Result<Vec<Vec<u8>>> {
        let node = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        Ok(node.storage.state_machine_log().await)
    }

    pub async fn wait_for_replication(
        &self,
        target_index: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut all_caught_up = true;
            for n in &self.nodes {
                if n.storage.state_machine_len().await <= target_index {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.leader().await.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Trigger openraft to take a snapshot on this node's local Raft
    /// instance. Once the snapshot is committed, openraft prunes log
    /// entries past `max_in_snapshot_log_to_keep` — so a follower
    /// that comes back online far enough behind has to catch up via
    /// `InstallSnapshot` instead of log replay.
    ///
    /// The state-machine on-disk log (qx-topic data + apply cursor)
    /// is independent of the Raft log and survives the prune.
    pub async fn trigger_snapshot_on_self(&self) -> Result<()> {
        let n = self
            .nodes
            .first()
            .ok_or(RaftError::LeaderTimeout)?;
        n.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }
}

/// Per-node construction shared by the all-in-one [`GrpcCluster::bootstrap`]
/// and the multi-process [`GrpcCluster::start_node`]. Spawns the tonic
/// server for inter-node Raft RPCs and builds the openraft `Raft<T>`
/// instance.
async fn start_grpc_node_internal(
    id: NodeId,
    listener: tokio::net::TcpListener,
    addrs: HashMap<NodeId, String>,
    node_dir: PathBuf,
    apply_callback: Option<crate::store::ApplyCallback>,
    tls: Option<(crate::grpc::RaftServerTls, crate::grpc::RaftClientTls)>,
) -> Result<GrpcClusterNode> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .map_err(|e| RaftError::Initialise(e.to_string()))?,
    );
    let storage = PersistentStorage::open_with_callback(&node_dir, apply_callback).await?;
    let (log_store, state_machine) = openraft::storage::Adaptor::new(storage.clone());
    let factory = crate::grpc::RaftGrpcFactory {
        addrs,
        client_tls: tls.as_ref().map(|(_, c)| c.clone()),
    };
    let raft = Raft::new(id, config, factory, log_store, state_machine)
        .await
        .map_err(|e| RaftError::Initialise(e.to_string()))?;
    let raft = Arc::new(raft);

    let svc = crate::grpc::RaftGrpcService::new(raft.clone());
    let server_tls = tls.as_ref().map(|(s, _)| s.clone());
    tokio::spawn(async move {
        let _ = svc.serve_with_listener_tls(listener, server_tls).await;
    });

    Ok(GrpcClusterNode {
        id,
        raft,
        storage,
    })
}

/// Disk-backed cluster: each node has its own subdirectory under `dir`
/// containing a `PartitionLog` for the Raft log and a redb database for
/// metadata + state machine + snapshots. Surviving a process restart is the
/// whole point of this variant — see the `persistent_*_restart_*` tests.
pub struct PersistentClusterNode {
    pub id: NodeId,
    pub raft: Arc<Raft<TypeConfig>>,
    pub storage: PersistentStorage,
}

pub struct PersistentCluster {
    nodes: Vec<PersistentClusterNode>,
}

impl PersistentCluster {
    pub async fn bootstrap(dir: impl AsRef<Path>, num_nodes: u32) -> Result<Self> {
        Self::bootstrap_with_appliers(dir, num_nodes, Vec::new()).await
    }

    pub async fn bootstrap_with_appliers(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        appliers: Vec<crate::store::ApplyCallback>,
    ) -> Result<Self> {
        let cluster = Self::open_with_appliers(dir.as_ref(), num_nodes, appliers).await?;
        let mut members = BTreeMap::new();
        for id in 0..(num_nodes as NodeId) {
            members.insert(id, BasicNode::default());
        }
        cluster.nodes[0]
            .raft
            .initialize(members)
            .await
            .map_err(|e| RaftError::Initialise(e.to_string()))?;
        cluster.wait_for_leader(Duration::from_secs(5)).await?;
        Ok(cluster)
    }

    /// Open an existing cluster (no `initialize()` — assumes membership is on disk).
    pub async fn open(dir: impl AsRef<Path>, num_nodes: u32) -> Result<Self> {
        Self::open_with_appliers(dir, num_nodes, Vec::new()).await
    }

    /// Open with an optional per-node apply-callback. When a callback is set,
    /// the storage **replays every existing log entry into the callback**
    /// before the Raft instance starts, so external state (e.g. a derived
    /// in-memory view) is rebuilt deterministically across restarts.
    pub async fn open_with_appliers(
        dir: impl AsRef<Path>,
        num_nodes: u32,
        appliers: Vec<crate::store::ApplyCallback>,
    ) -> Result<Self> {
        assert!(num_nodes > 0);
        assert!(appliers.is_empty() || appliers.len() == num_nodes as usize);
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .map_err(|e| RaftError::Initialise(e.to_string()))?,
        );

        let registry: Registry = Arc::new(RwLock::new(HashMap::new()));
        let partitions_persistent: Partitions =
            Arc::new(std::sync::RwLock::new(std::collections::HashSet::new()));
        let mut nodes = Vec::with_capacity(num_nodes as usize);
        for id in 0..(num_nodes as NodeId) {
            let node_dir = dir.join(format!("node-{id}"));
            let cb = appliers.get(id as usize).cloned();
            let storage = PersistentStorage::open_with_callback(&node_dir, cb.clone()).await?;
            // Replay existing log so external state matches persisted state.
            if let Some(cb) = cb {
                storage
                    .replay_to_callback(move |idx, bytes| cb(idx, bytes))
                    .await?;
            }
            let (log_store, state_machine) =
                openraft::storage::Adaptor::new(storage.clone());
            let factory = InProcessFactory {
                source: id,
                registry: registry.clone(),
                partitions: partitions_persistent.clone(),
            };
            let raft = Raft::new(id, config.clone(), factory, log_store, state_machine)
                .await
                .map_err(|e| RaftError::Initialise(e.to_string()))?;
            let raft = Arc::new(raft);
            registry.write().await.insert(id, raft.clone());
            nodes.push(PersistentClusterNode {
                id,
                raft,
                storage,
            });
        }
        Ok(Self { nodes })
    }

    pub fn size(&self) -> u32 {
        self.nodes.len() as u32
    }

    pub async fn leader(&self) -> Option<NodeId> {
        for node in &self.nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if let Some(leader) = metrics.current_leader {
                return Some(leader);
            }
        }
        None
    }

    pub async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        // Multi-mode semantics:
        //   - bootstrap-all-nodes-here: pick the openraft-reported
        //     leader from `self.nodes` and call client_write.
        //   - single-node-per-process (start_node): `self.nodes` has
        //     only this broker's Raft instance. Call client_write on
        //     it; openraft surfaces ForwardToLeader through the error
        //     path when this isn't the leader. Clients retry against
        //     the real leader broker — the same pattern Kafka uses.
        let leader = self.leader().await.ok_or(RaftError::LeaderTimeout)?;
        let target = self
            .nodes
            .iter()
            .find(|n| n.id == leader)
            .or_else(|| self.nodes.first())
            .ok_or(RaftError::UnknownNode(leader))?;
        let resp = target
            .raft
            .client_write(AppRequest { payload })
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(resp.data.index)
    }

    pub async fn ensure_linearizable(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .ensure_linearizable()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    pub async fn shutdown_node(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .shutdown()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    pub async fn state_log(&self, node: NodeId) -> Result<Vec<Vec<u8>>> {
        let node = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        Ok(node.storage.state_machine_log().await)
    }

    pub async fn wait_for_replication(
        &self,
        target_index: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let mut all_caught_up = true;
            for n in &self.nodes {
                if n.storage.state_machine_len().await <= target_index {
                    all_caught_up = false;
                    break;
                }
            }
            if all_caught_up {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        for node in self.nodes {
            // openraft Raft has `shutdown()` which signals workers and waits
            // for them. Failures during shutdown shouldn't block the test.
            let _ = node.raft.shutdown().await;
        }
        Ok(())
    }

    /// Manually trigger an openraft snapshot on `node`. After the snapshot
    /// applies, openraft will purge log entries past `max_in_snapshot_log_to_keep`.
    /// The state machine log on disk continues to reflect every applied entry,
    /// so restart-via-`replay_to_callback` still recovers full state.
    pub async fn trigger_snapshot(&self, node: NodeId) -> Result<()> {
        let n = self
            .nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or(RaftError::UnknownNode(node))?;
        n.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| RaftError::ClientWrite(e.to_string()))?;
        Ok(())
    }

    async fn wait_for_leader(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.leader().await.is_some() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RaftError::LeaderTimeout);
            }
            sleep(Duration::from_millis(20)).await;
        }
    }
}
