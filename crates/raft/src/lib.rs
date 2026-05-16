//! qx-raft — openraft-backed replicated cluster.
//!
//! Provides an in-process 3+-node consensus cluster using
//! [`openraft`](https://docs.rs/openraft/0.9). The state machine is a simple
//! append-only log of byte payloads — the contract a production QX broker
//! would target. Storage and network are in-memory / in-process; persistent
//! storage and a real network transport are separate engineering items.

mod cluster;
mod error;
pub mod grpc;
mod network;
mod persistent_store;
mod replicated_topic;
mod store;
mod types;

pub use cluster::{Cluster, GrpcCluster, PersistentCluster};
pub use grpc::{RaftClientTls, RaftServerTls};
pub use error::{RaftError, Result};
pub use persistent_store::PersistentStorage;
pub use replicated_topic::{IsolationLevel, PersistentReplicatedTopic, ReplicatedTopic};
pub use types::{AppRequest, AppResponse, NodeId, TypeConfig};
