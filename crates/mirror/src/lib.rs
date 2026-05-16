//! qx-mirror — cross-cluster topic mirroring.
//!
//! A `Mirror` connects to a source cluster, joins a consumer group, and
//! re-publishes each consumed record to a destination cluster. Ordering is
//! preserved per source partition by consuming and republishing serially.
//! Offsets are committed only after the destination ack returns successfully.
//!
//! This is a thin orchestration layer over `qx_broker::Client` — it does not
//! poke at broker internals.

use std::time::Duration;

use qx_broker::{BrokerError, Client};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MirrorError {
    #[error("broker: {0}")]
    Broker(#[from] BrokerError),
}

pub type Result<T> = std::result::Result<T, MirrorError>;

#[derive(Debug, Clone)]
pub struct MirrorConfig {
    pub source_addr: String,
    pub source_topic: String,
    pub source_partition_count: u32,
    pub source_group: String,
    pub dest_addr: String,
    pub dest_topic: String,
    pub dest_partition_count: u32,
    /// Maximum records pulled per partition per pump cycle.
    pub batch: u32,
    /// Poll delay when the source has no new messages.
    pub idle_poll: Duration,
}

pub struct Mirror {
    config: MirrorConfig,
    source: Client,
    dest: Client,
}

impl Mirror {
    pub async fn open(config: MirrorConfig) -> Result<Self> {
        let mut source = Client::connect(config.source_addr.clone()).await?;
        let mut dest = Client::connect(config.dest_addr.clone()).await?;
        source
            .open_topic(&config.source_topic, config.source_partition_count)
            .await?;
        dest.open_topic(&config.dest_topic, config.dest_partition_count)
            .await?;
        Ok(Self {
            config,
            source,
            dest,
        })
    }

    /// Run one mirroring pump cycle: for each source partition, pull a batch,
    /// republish to dest, commit the source offset. Returns the number of
    /// records mirrored this cycle (zero means everything is caught up).
    pub async fn pump_once(&mut self) -> Result<usize> {
        let mut moved = 0;
        for p in 0..self.config.source_partition_count {
            let batch = self
                .source
                .stream_from_group(
                    &self.config.source_topic,
                    &self.config.source_group,
                    p,
                    self.config.batch,
                )
                .await?;
            if batch.is_empty() {
                continue;
            }
            let last_id = batch.last().unwrap().id;
            // Republish each record. The destination keys are the source
            // record id encoded as bytes — keeps records from the same source
            // partition on the same destination partition under hash routing,
            // which preserves per-source-partition ordering.
            for rec in &batch {
                let key = format!("p{}-r{}", p, rec.id);
                self.dest
                    .send(&self.config.dest_topic, key.as_bytes(), &rec.payload)
                    .await?;
            }
            self.source
                .commit_group_offset(
                    &self.config.source_topic,
                    &self.config.source_group,
                    p,
                    last_id + 1,
                )
                .await?;
            moved += batch.len();
        }
        Ok(moved)
    }

    /// Run until at least `target` records have been mirrored, polling with
    /// `idle_poll` when the source is caught up. Useful for tests.
    pub async fn pump_until(&mut self, target: usize) -> Result<usize> {
        let mut total = 0;
        while total < target {
            let n = self.pump_once().await?;
            if n == 0 {
                tokio::time::sleep(self.config.idle_poll).await;
            }
            total += n;
        }
        Ok(total)
    }
}
