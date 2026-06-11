//! Cluster abstraction shared by [`crate::Cluster`] (in-process),
//! [`crate::GrpcCluster`] (real gRPC transport), and [`crate::PersistentCluster`]
//! (persistent storage, in-process transport).
//!
//! The broker's [`qx_broker::replication::RaftReplicator`] holds an
//! `Arc<dyn RaftCluster>` so the same replicator works against either
//! the in-process test fixture or a real gRPC deployment without
//! recompiling.

use async_trait::async_trait;

use crate::error::Result;
use crate::types::NodeId;

#[async_trait]
pub trait RaftCluster: Send + Sync + 'static {
    /// Propose `payload` through the cluster's current leader and wait
    /// until the leader has both committed (majority replication) and
    /// applied the entry locally. Returns the Raft log index.
    async fn append(&self, payload: Vec<u8>) -> Result<u64>;

    /// Current Raft leader, if any node has elected one.
    async fn leader(&self) -> Option<NodeId>;
}

#[async_trait]
impl RaftCluster for crate::Cluster {
    async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        crate::Cluster::append(self, payload).await
    }

    async fn leader(&self) -> Option<NodeId> {
        crate::Cluster::leader(self).await
    }
}

#[async_trait]
impl RaftCluster for crate::GrpcCluster {
    async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        crate::GrpcCluster::append(self, payload).await
    }

    async fn leader(&self) -> Option<NodeId> {
        crate::GrpcCluster::leader(self).await
    }
}

#[async_trait]
impl RaftCluster for crate::PersistentCluster {
    async fn append(&self, payload: Vec<u8>) -> Result<u64> {
        crate::PersistentCluster::append(self, payload).await
    }

    async fn leader(&self) -> Option<NodeId> {
        crate::PersistentCluster::leader(self).await
    }
}
