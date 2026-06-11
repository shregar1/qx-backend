//! Periodic background task that auto-aborts transactions whose
//! `timeout_at` has passed. Spawned by `Broker::start_background`.
//!
//! Real Kafka calls this the "transaction abort timeout" sweep. A
//! producer can crash mid-transaction; without this task the in-flight
//! records would stay hidden from READ_COMMITTED consumers forever.

use std::sync::Arc;
use std::time::Duration;

use crate::server::Broker;

/// How often to sweep. Tight enough that a misbehaving producer doesn't
/// pin records for long; loose enough that the lock isn't contended in
/// hot paths.
const SWEEP_INTERVAL: Duration = Duration::from_secs(2);

pub fn spawn(broker: Arc<Broker>) {
    let mut shutdown = broker.shutdown_token();
    let registered_broker = broker.clone();
    let handle = tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let registry = broker.txn_registry();
            // Sweep is cheap and synchronous; run it on the runtime thread.
            let aborted = tokio::task::spawn_blocking(move || registry.sweep_expired())
                .await
                .unwrap_or(0);
            if aborted > 0 {
                let msg = format!(
                    "txn sweeper: auto-aborted {aborted} expired transaction(s)"
                );
                eprintln!("{msg}");
                broker.emit_event(crate::admin_api::EventLevel::Warn, "txn", msg);
            }
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
        }
    });
    registered_broker.register_background(handle);
}
