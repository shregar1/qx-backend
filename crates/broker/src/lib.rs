//! qx-broker — gRPC broker that exposes `qx_topic::Topic` over the network.
//!
//! Public API:
//!   `Broker`  — server-side state holder; `Broker::open(dir)` + `.serve(addr)`.
//!   `Client`  — high-level client over the generated tonic stub.

mod client;
mod error;
mod groups;
mod server;

pub mod proto {
    tonic::include_proto!("qx.v1");
}

pub use client::Client;
pub use error::{BrokerError, Result};
pub use server::{Broker, BrokerConfig, TlsConfig};
