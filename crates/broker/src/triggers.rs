//! Topic-to-workflow triggers.
//!
//! A trigger durably binds `(topic, consumer group, workflow name)`. While
//! the broker is running, one tokio task per registered trigger consumes
//! the topic group and creates a workflow task for each record. The task
//! creation is idempotent (keyed by `topic:partition:record_id`), so
//! consumer-side retries cannot duplicate tasks even if the broker crashes
//! between creating a task and committing the offset.
//!
//! Triggers themselves are persisted in a redb file under `data_dir/triggers.redb`
//! so they survive broker restart.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use redb::{Database, ReadableTable, TableDefinition};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::{BrokerError, Result};
use crate::server::Broker;

const TRIGGERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("triggers");

/// Durable definition of one trigger. Encoded into the triggers redb.
#[derive(Debug, Clone)]
pub struct TriggerConfig {
    pub name: String,
    pub topic: String,
    pub group: String,
    pub workflow: String,
    pub batch_size: u32,
    pub poll_interval_ms: u64,
}

impl TriggerConfig {
    pub fn new(name: &str, topic: &str, workflow: &str) -> Self {
        Self {
            name: name.to_string(),
            topic: topic.to_string(),
            // Each trigger uses a stable group name so its offset advances
            // independently of any human consumer using the same topic.
            group: format!("__trigger_{name}"),
            workflow: workflow.to_string(),
            batch_size: 32,
            poll_interval_ms: 100,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        write_str(&mut out, &self.name);
        write_str(&mut out, &self.topic);
        write_str(&mut out, &self.group);
        write_str(&mut out, &self.workflow);
        out.extend_from_slice(&self.batch_size.to_le_bytes());
        out.extend_from_slice(&self.poll_interval_ms.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut c = 0;
        let name = read_str(bytes, &mut c)?;
        let topic = read_str(bytes, &mut c)?;
        let group = read_str(bytes, &mut c)?;
        let workflow = read_str(bytes, &mut c)?;
        if bytes.len() < c + 4 + 8 {
            return Err(BrokerError::Custom("corrupt trigger record".into()));
        }
        let batch_size =
            u32::from_le_bytes(bytes[c..c + 4].try_into().unwrap());
        c += 4;
        let poll_interval_ms =
            u64::from_le_bytes(bytes[c..c + 8].try_into().unwrap());
        Ok(Self { name, topic, group, workflow, batch_size, poll_interval_ms })
    }
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_str(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    if bytes.len() < *cursor + 4 {
        return Err(BrokerError::Custom("corrupt trigger record".into()));
    }
    let len = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap()) as usize;
    *cursor += 4;
    if bytes.len() < *cursor + len {
        return Err(BrokerError::Custom("corrupt trigger record".into()));
    }
    let s = String::from_utf8(bytes[*cursor..*cursor + len].to_vec())
        .map_err(|_| BrokerError::Custom("trigger record has invalid utf-8".into()))?;
    *cursor += len;
    Ok(s)
}

/// Live handle to a running trigger. Dropping this without calling `stop`
/// will abort the dispatcher task — usually fine on broker shutdown, but
/// callers should prefer `stop().await` so the in-flight batch completes.
#[allow(dead_code)] // graceful shutdown is wired but not yet called from anywhere
pub struct TriggerHandle {
    pub config: TriggerConfig,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl TriggerHandle {
    #[allow(dead_code)]
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for TriggerHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Persistent store for triggers. Wraps a redb file under data_dir.
pub struct TriggerStore {
    db: Database,
}

impl TriggerStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join("triggers.redb");
        let db = Database::create(&path).map_err(redb_err)?;
        let txn = db.begin_write().map_err(redb_err)?;
        txn.open_table(TRIGGERS_TABLE).map_err(redb_err)?;
        txn.commit().map_err(redb_err)?;
        Ok(Self { db })
    }

    pub fn put(&self, cfg: &TriggerConfig) -> Result<()> {
        let txn = self.db.begin_write().map_err(redb_err)?;
        {
            let mut t = txn.open_table(TRIGGERS_TABLE).map_err(redb_err)?;
            let bytes = cfg.encode();
            t.insert(cfg.name.as_str(), bytes.as_slice()).map_err(redb_err)?;
        }
        txn.commit().map_err(redb_err)?;
        Ok(())
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        let txn = self.db.begin_write().map_err(redb_err)?;
        let removed = {
            let mut t = txn.open_table(TRIGGERS_TABLE).map_err(redb_err)?;
            t.remove(name).map_err(redb_err)?.is_some()
        };
        txn.commit().map_err(redb_err)?;
        Ok(removed)
    }

    pub fn list(&self) -> Result<Vec<TriggerConfig>> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        let t = txn.open_table(TRIGGERS_TABLE).map_err(redb_err)?;
        let mut out = Vec::new();
        for entry in t.iter().map_err(redb_err)? {
            let (_k, v) = entry.map_err(redb_err)?;
            out.push(TriggerConfig::decode(v.value())?);
        }
        Ok(out)
    }
}

fn redb_err<E: std::fmt::Display>(e: E) -> BrokerError {
    BrokerError::Custom(format!("triggers redb: {e}"))
}

/// Spawn the dispatcher loop for a trigger. Returns a handle the caller can
/// use to stop it cleanly.
pub fn spawn_dispatcher(broker: Arc<Broker>, config: TriggerConfig) -> TriggerHandle {
    let (tx, rx) = oneshot::channel();
    let cfg = config.clone();
    let task = tokio::spawn(async move {
        dispatch_loop(broker, cfg, rx).await;
    });
    TriggerHandle {
        config,
        shutdown: Some(tx),
        task: Some(task),
    }
}

async fn dispatch_loop(
    broker: Arc<Broker>,
    config: TriggerConfig,
    mut shutdown: oneshot::Receiver<()>,
) {
    let poll = Duration::from_millis(config.poll_interval_ms.max(10));
    let mut broker_shutdown = broker.shutdown_token();
    loop {
        if shutdown.try_recv().is_ok() || *broker_shutdown.borrow() {
            return;
        }

        let bk = broker.clone();
        let cfg = config.clone();
        let result =
            tokio::task::spawn_blocking(move || run_iteration(&bk, &cfg)).await;

        let processed = match result {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                let msg = format!("trigger '{}': iteration error: {}", config.name, e);
                eprintln!("{msg}");
                broker.emit_event(
                    crate::admin_api::EventLevel::Error,
                    "triggers",
                    msg,
                );
                0
            }
            Err(e) => {
                let msg = format!("trigger '{}': join error: {}", config.name, e);
                eprintln!("{msg}");
                broker.emit_event(
                    crate::admin_api::EventLevel::Error,
                    "triggers",
                    msg,
                );
                0
            }
        };

        if processed == 0 {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = broker_shutdown.changed() => return,
                _ = tokio::time::sleep(poll) => {}
            }
        }
    }
}

fn run_iteration(broker: &Broker, cfg: &TriggerConfig) -> Result<usize> {
    let topic = broker
        .lookup(&cfg.topic)
        .ok_or_else(|| BrokerError::TopicNotFound(cfg.topic.clone()))?;
    let workflow = broker.get_or_open_workflow(&cfg.workflow)?;

    let partition_count = topic.partition_count();
    let mut total = 0usize;

    for partition in 0..partition_count {
        let msgs = topic.stream(&cfg.group, partition, cfg.batch_size as usize)?;
        if msgs.is_empty() {
            continue;
        }
        let last_id = msgs.last().unwrap().id;

        for msg in &msgs {
            let dedup_key = make_dedup_key(&cfg.topic, partition, msg.id);
            let mut wf = workflow.lock().unwrap();
            wf.create_root_idempotent(&dedup_key, &msg.payload)?;
        }

        // Group's next offset = last_id + 1.
        topic.commit_offset(&cfg.group, partition, last_id + 1)?;
        total += msgs.len();
    }

    Ok(total)
}

fn make_dedup_key(topic: &str, partition: u32, record_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(topic.len() + 1 + 4 + 8);
    k.extend_from_slice(topic.as_bytes());
    k.push(b':');
    k.extend_from_slice(&partition.to_le_bytes());
    k.extend_from_slice(&record_id.to_le_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_config_round_trips() {
        let cfg = TriggerConfig {
            name: "orders-to-process".into(),
            topic: "orders".into(),
            group: "__trigger_orders-to-process".into(),
            workflow: "process_order".into(),
            batch_size: 64,
            poll_interval_ms: 250,
        };
        let bytes = cfg.encode();
        let decoded = TriggerConfig::decode(&bytes).unwrap();
        assert_eq!(decoded.name, cfg.name);
        assert_eq!(decoded.topic, cfg.topic);
        assert_eq!(decoded.workflow, cfg.workflow);
        assert_eq!(decoded.batch_size, cfg.batch_size);
        assert_eq!(decoded.poll_interval_ms, cfg.poll_interval_ms);
    }

    #[test]
    fn dedup_keys_differ_by_partition_and_offset() {
        let a = make_dedup_key("t", 0, 1);
        let b = make_dedup_key("t", 0, 2);
        let c = make_dedup_key("t", 1, 1);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, make_dedup_key("t", 0, 1));
    }
}
