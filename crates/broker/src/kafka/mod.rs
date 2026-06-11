//! Kafka wire-protocol surface for the broker.
//!
//! This module implements just enough of the Kafka client protocol that
//! standard tooling (`kcat`, `kafka-console-*`, `librdkafka`-based clients)
//! can connect and discover the cluster. Today that's `ApiVersions` (key 18)
//! and `Metadata` (key 3); more APIs will land here as separate submodules.

pub(crate) mod acls;
mod admin_stubs;
mod alter_configs;
mod api_versions;
mod codec;
mod create_partitions;
mod create_topics;
mod delete_records;
mod delete_topics;
mod describe_cluster;
mod describe_configs;
mod describe_quorum;
mod error_codes;
mod fetch;
mod group_apis;
pub(crate) mod group_coordinator;
mod header;
mod incremental_alter_configs;
mod list_offsets;
mod log_dirs;
mod metadata;
mod offset_delete;
mod offset_for_leader_epoch;
pub(crate) mod produce;
mod sasl;
pub(crate) mod scram;
pub(crate) mod produce_writer;
pub(crate) mod producer_dedup;
pub(crate) mod shards;
mod record_batch;
mod server;
pub(crate) mod transactions;
mod txn_introspect;
pub(crate) mod txn_store;

pub use scram::ScramCredential;
pub use server::KafkaConfig;

/// In-memory state we keep per `(producer_id, partition)` so that retries
/// from a Kafka idempotent producer are recognized as duplicates rather
/// than re-appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerSequenceState {
    pub last_base_sequence: i32,
    pub last_record_count: i32,
    pub last_base_offset: i64,
}
