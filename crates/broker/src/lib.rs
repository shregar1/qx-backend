//! qx-broker — gRPC broker that exposes `qx_topic::Topic` over the network.
//!
//! Public API:
//!   `Broker`  — server-side state holder; `Broker::open(dir)` + `.serve(addr)`.
//!   `Client`  — high-level client over the generated tonic stub.

pub mod admin_api;
mod client;
mod dashboard_assets;
mod error;
mod groups;
mod kafka;
mod outbox;
pub mod replication;
mod server;
mod triggers;
mod txn_sweeper;

pub mod proto {
    tonic::include_proto!("qx.v1");
}

pub use client::Client;
pub use error::{BrokerError, Result};
pub use kafka::produce::dump_produce_timings;
pub use kafka::{KafkaConfig, ScramCredential};
pub use replication::{NoopReplicator, ProduceCommand, Replicator};
pub use server::{Broker, BrokerConfig, BrokerMode, ClusterConfig, TlsConfig, bind_with_retry};
pub use triggers::TriggerConfig;
