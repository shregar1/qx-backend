//! In-process `RaftNetworkFactory` + `RaftNetwork`. Each node holds a clone
//! of a shared registry mapping `NodeId -> Arc<Raft<TypeConfig>>`; sending
//! an RPC is a direct method call on the peer's `Raft` instance.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use openraft::error::InstallSnapshotError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::AnyError;
use openraft::BasicNode;
use openraft::Raft;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig};

pub type Registry = Arc<RwLock<HashMap<NodeId, Arc<Raft<TypeConfig>>>>>;
pub type Partitions = Arc<std::sync::RwLock<HashSet<(NodeId, NodeId)>>>;

#[derive(Clone)]
pub struct InProcessFactory {
    pub source: NodeId,
    pub registry: Registry,
    pub partitions: Partitions,
}

#[derive(Clone)]
pub struct InProcessNetwork {
    pub source: NodeId,
    pub target: NodeId,
    pub registry: Registry,
    pub partitions: Partitions,
}

impl RaftNetworkFactory<TypeConfig> for InProcessFactory {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InProcessNetwork {
            source: self.source,
            target,
            registry: self.registry.clone(),
            partitions: self.partitions.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.check_partition()?;
        let raft = self.lookup_target().await?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.check_partition_snapshot()?;
        let raft = self.lookup_target_for_snapshot().await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        self.check_partition()?;
        let raft = self.lookup_target().await?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }
}

impl InProcessNetwork {
    fn is_partitioned(&self, partitions: &HashSet<(NodeId, NodeId)>) -> bool {
        partitions.contains(&(self.source, self.target))
            || partitions.contains(&(self.target, self.source))
    }

    #[allow(clippy::result_large_err)]
    fn check_partition(
        &self,
    ) -> std::result::Result<(), RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let partitions = self.partitions.read().expect("partitions lock poisoned");
        if self.is_partitioned(&partitions) {
            return Err(RPCError::Unreachable(Unreachable::new(&AnyError::error(
                format!("partition: {} ↮ {}", self.source, self.target),
            ))));
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn check_partition_snapshot(
        &self,
    ) -> std::result::Result<
        (),
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let partitions = self.partitions.read().expect("partitions lock poisoned");
        if self.is_partitioned(&partitions) {
            return Err(RPCError::Unreachable(Unreachable::new(&AnyError::error(
                format!("partition: {} ↮ {}", self.source, self.target),
            ))));
        }
        Ok(())
    }

    async fn lookup_target(
        &self,
    ) -> std::result::Result<Arc<Raft<TypeConfig>>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    {
        self.registry
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(format!(
                    "node {} not in registry",
                    self.target
                ))))
            })
    }

    async fn lookup_target_for_snapshot(
        &self,
    ) -> std::result::Result<
        Arc<Raft<TypeConfig>>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.registry
            .read()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(format!(
                    "node {} not in registry",
                    self.target
                ))))
            })
    }
}
