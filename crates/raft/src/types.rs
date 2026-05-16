//! Raft type configuration: application data + response, NodeId, snapshot type.

use std::io::Cursor;

use openraft::declare_raft_types;
use openraft::BasicNode;
use openraft::TokioRuntime;
use serde::Deserialize;
use serde::Serialize;

pub type NodeId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppRequest {
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppResponse {
    pub index: u64,
}

declare_raft_types!(
    pub TypeConfig:
        D = AppRequest,
        R = AppResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);
