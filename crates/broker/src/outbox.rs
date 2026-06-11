//! Workflow → topic outbox dispatcher.
//!
//! One global tokio task polls every open workflow, drains its outbox of
//! pending emits, and writes each payload to the named target topic. On
//! success the entry is removed from the outbox; on failure (topic doesn't
//! exist, append errors) the entry stays and is retried on the next tick.
//!
//! Partition selection: stable hash of `task_id` so all emits from a given
//! task land on the same partition. That preserves intra-task ordering for
//! downstream consumers that care.

use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;
use crate::server::Broker;

const POLL_INTERVAL_MS: u64 = 100;

/// Spawn the global outbox dispatcher. Idempotent at the call site — calling
/// twice will spawn two dispatchers; both will be correct but you'll waste
/// CPU. `Broker::start_background` only calls it once.
pub fn spawn(broker: Arc<Broker>) {
    let mut shutdown = broker.shutdown_token();
    let registered_broker = broker.clone();
    let handle = tokio::spawn(async move {
        let poll = Duration::from_millis(POLL_INTERVAL_MS);
        loop {
            if *shutdown.borrow() {
                return;
            }
            let bk = broker.clone();
            let processed =
                match tokio::task::spawn_blocking(move || drain_all_outboxes(&bk)).await {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        eprintln!("outbox dispatcher: {e}");
                        0
                    }
                    Err(e) => {
                        eprintln!("outbox dispatcher join error: {e}");
                        0
                    }
                };
            if processed == 0 {
                tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = tokio::time::sleep(poll) => {}
                }
            }
        }
    });
    registered_broker.register_background(handle);
}

fn drain_all_outboxes(broker: &Broker) -> Result<usize> {
    let names = broker.open_workflow_names();
    let mut total = 0;
    for name in names {
        let Some(workflow) = broker.lookup_workflow(&name) else {
            continue;
        };
        let pending = {
            let wf = workflow.lock().unwrap();
            wf.pending_emits()?
        };
        for emit in pending {
            let Some(topic) = broker.lookup(&emit.target_topic) else {
                // Target topic doesn't exist yet. Skip — we'll retry next tick.
                continue;
            };
            let partition = (emit.task_id as u32) % topic.partition_count().max(1);
            // `emit.payload` is already `Bytes` — pass it through
            // the refcount-aware variant so the dispatcher doesn't
            // borrow-then-realloc the buffer on the way to the
            // queue's `send`.
            let send = topic.send_to_bytes(partition, emit.payload.clone());
            if send.is_ok() {
                workflow.lock().unwrap().mark_emit_delivered(emit.emit_id)?;
                total += 1;
            } else {
                eprintln!(
                    "outbox: failed to deliver emit {} to {} (will retry)",
                    emit.emit_id, emit.target_topic
                );
            }
        }
    }
    Ok(total)
}
