//! Durable engine for task DAGs.
//!
//! Two redb tables back the engine:
//!   `tasks: TaskId -> encoded Task`
//!   `children: parent_id -> child_id` (multimap, ordered by insert)
//!   `meta: &str -> u64`  (holds the next task id to allocate)

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, MultimapTableDefinition, ReadableTable, TableDefinition};

use crate::error::{Result, WorkflowError};
use crate::task::{Task, TaskId, TaskStatus};

const TASKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("tasks");
const CHILDREN_TABLE: MultimapTableDefinition<u64, u64> =
    MultimapTableDefinition::new("children");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");
const NEXT_ID_KEY: &str = "next_id";
/// `dedup_key -> TaskId`. Used by `create_root_idempotent` so triggers
/// (and any other at-least-once caller) can de-duplicate task creation
/// without coordinating outside the engine.
const DEDUP_TABLE: TableDefinition<&[u8], u64> = TableDefinition::new("dedup");
/// Outbox: `emit_id -> encoded(PendingEmit)`. A task may stage one or more
/// emits in the same write transaction as its completion. A separate
/// dispatcher (broker-side) drains pending entries and removes them once
/// they've been delivered to the target topic.
const OUTBOX_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("outbox");
const NEXT_EMIT_KEY: &str = "next_emit_id";

pub struct Engine {
    db: Database,
    /// Scratch buffer reused across every redb-insert encoding step
    /// inside one logical operation. `Task::encode_into` and
    /// `PendingEmit::encode_into` both clear-and-extend this buffer
    /// rather than allocating a fresh `Vec` per write. Over a fan-out
    /// of N emits, total Vec growth is O(max(emit_size)), not
    /// O(sum(emit_size)).
    ///
    /// `&mut self` is required for every public Engine method that
    /// writes, so concurrent access is already impossible — no lock
    /// is needed around the scratch buffer.
    encode_buf: Vec<u8>,
    /// Captured from [`EngineOptions::cancel_max_descendants`] at
    /// open. `0` means unlimited.
    cancel_max_descendants: usize,
}

/// Per-engine knobs supplied to [`Engine::with_options`]. Default
/// values preserve [`Engine::open`]'s historic behaviour.
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Initial capacity for the engine-internal `encode_buf` scratch.
    /// Sized to cover the typical task's header + payload + result
    /// so the first `create` / `complete_with_emits` doesn't pay a
    /// growth-from-0 cost. Workloads with consistently-large
    /// payloads should set this to their p99 task size; small
    /// workloads can lower it.
    pub encode_buf_capacity: usize,
    /// Upper bound on the number of descendants `Engine::cancel`
    /// will collect before refusing the operation. Defends against a
    /// runaway task tree (corruption, a bug that creates children
    /// in a cycle, etc.). `0` disables the guard — the historic
    /// behaviour, unbounded.
    pub cancel_max_descendants: usize,
    /// Optional redb page cache size, in bytes. When `Some`, the
    /// engine's redb database is built via
    /// `Builder::set_cache_size(...)`. Larger caches reduce disk
    /// reads on workflows that repeatedly inspect the same tasks;
    /// `None` accepts redb's own default.
    pub redb_cache_bytes: Option<usize>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            encode_buf_capacity: 1024,
            cancel_max_descendants: 0,
            redb_cache_bytes: None,
        }
    }
}

/// Aggregate counts produced by [`Engine::stats`].
#[derive(Debug, Default, Clone, Copy)]
pub struct EngineStats {
    pub total: u64,
    pub pending: u64,
    pub running: u64,
    pub done: u64,
    pub failed: u64,
    pub cancelled: u64,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_options(path, EngineOptions::default())
    }

    /// Open with a caller-supplied [`EngineOptions`]. Operators with
    /// known-large task payloads can set
    /// [`EngineOptions::encode_buf_capacity`] to their p99 task size
    /// so even the first redb insert avoids growing the scratch.
    pub fn with_options(
        path: impl AsRef<Path>,
        options: EngineOptions,
    ) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Use redb's `Builder` so the caller can size the page cache.
        // `Database::create` is shorthand for "builder with defaults
        // → create or open"; we replicate it via the builder to get
        // at the cache knob.
        let mut builder = redb::Builder::new();
        if let Some(cache) = options.redb_cache_bytes {
            builder.set_cache_size(cache);
        }
        let db = builder.create(path)?;
        ensure_schema(&db)?;
        Ok(Self {
            db,
            encode_buf: Vec::with_capacity(options.encode_buf_capacity),
            cancel_max_descendants: options.cancel_max_descendants,
        })
    }

    pub fn create_root(&mut self, payload: &[u8]) -> Result<TaskId> {
        self.create(None, payload)
    }

    pub fn create_child(&mut self, parent_id: TaskId, payload: &[u8]) -> Result<TaskId> {
        self.create(Some(parent_id), payload)
    }

    /// Symmetric Bytes-accepting variants of [`Self::create_root`] /
    /// [`Self::create_child`]. The bytes go straight through to the
    /// redb insert via `payload.as_ref()`, so a caller that already
    /// holds a refcounted slice doesn't pay an extra `Vec<u8>`
    /// conversion to call into the engine.
    pub fn create_root_bytes(&mut self, payload: &bytes::Bytes) -> Result<TaskId> {
        self.create(None, payload.as_ref())
    }

    pub fn create_child_bytes(
        &mut self,
        parent_id: TaskId,
        payload: &bytes::Bytes,
    ) -> Result<TaskId> {
        self.create(Some(parent_id), payload.as_ref())
    }

    /// Create a root task only if `dedup_key` hasn't been seen before. If
    /// the key already maps to a task, return that existing task id without
    /// any state change.
    ///
    /// Use this from at-least-once delivery paths (e.g. topic triggers) so
    /// retries don't multiply tasks. The dedup table and the task insert
    /// share a single redb write transaction, so the "did this key already
    /// land?" check is atomic with the task creation.
    pub fn create_root_idempotent(
        &mut self,
        dedup_key: &[u8],
        payload: &[u8],
    ) -> Result<TaskId> {
        let write_txn = self.db.begin_write()?;

        // Look up first in an inner scope so all redb borrows drop before
        // we touch other tables or commit.
        let existing: Option<TaskId> = {
            let dedup = write_txn.open_table(DEDUP_TABLE)?;
            dedup.get(dedup_key)?.map(|g| g.value())
        };

        let final_id = match existing {
            Some(id) => id,
            None => {
                let id = {
                    let mut meta = write_txn.open_table(META_TABLE)?;
                    let current = meta.get(NEXT_ID_KEY)?.map(|g| g.value()).unwrap_or(0);
                    meta.insert(NEXT_ID_KEY, current + 1)?;
                    current
                };
                {
                    let mut tasks = write_txn.open_table(TASKS_TABLE)?;
                    let task = Task {
                        id,
                        parent_id: None,
                        status: TaskStatus::Pending,
                        created_nanos: now_nanos(),
                        payload: bytes::Bytes::copy_from_slice(payload),
                        result: None,
                        error: None,
                    };
                    task.encode_into(&mut self.encode_buf);
                    tasks.insert(id, self.encode_buf.as_slice())?;
                }
                {
                    let mut dedup = write_txn.open_table(DEDUP_TABLE)?;
                    dedup.insert(dedup_key, id)?;
                }
                id
            }
        };

        write_txn.commit()?;
        Ok(final_id)
    }

    pub fn mark_running(&mut self, id: TaskId) -> Result<()> {
        self.update(id, |task| {
            if task.status.is_terminal() {
                Err(WorkflowError::TerminalTask { id })
            } else {
                task.status = TaskStatus::Running;
                Ok(())
            }
        })
    }

    pub fn complete(
        &mut self,
        id: TaskId,
        result: impl Into<bytes::Bytes>,
    ) -> Result<()> {
        let result: bytes::Bytes = result.into();
        self.update(id, |task| {
            if task.status.is_terminal() {
                Err(WorkflowError::TerminalTask { id })
            } else {
                task.status = TaskStatus::Done;
                // `Bytes::from(vec)` / `Bytes::clone()` are both
                // refcount/move operations — no realloc per call.
                task.result = Some(result.clone());
                Ok(())
            }
        })
    }

    /// Complete a task and atomically stage one or more topic emits for
    /// delivery. The task transition and the outbox inserts share a single
    /// write transaction, so the completion is never visible without its
    /// emits (transactional outbox pattern).
    pub fn complete_with_emits(
        &mut self,
        id: TaskId,
        result: impl Into<bytes::Bytes>,
        emits: &[(&str, bytes::Bytes)],
    ) -> Result<Vec<u64>> {
        let result: bytes::Bytes = result.into();
        let write_txn = self.db.begin_write()?;
        {
            let mut tasks = write_txn.open_table(TASKS_TABLE)?;
            // Move the redb-read bytes straight into a `Bytes`, then
            // let `decode_from` slice `payload`/`result` views into
            // that single allocation via refcount bumps.
            let buf = bytes::Bytes::from(
                tasks
                    .get(id)?
                    .ok_or(WorkflowError::TaskNotFound { id })?
                    .value()
                    .to_vec(),
            );
            let mut task = Task::decode_from(id, buf)?;
            if task.status.is_terminal() {
                return Err(WorkflowError::TerminalTask { id });
            }
            task.status = TaskStatus::Done;
            task.result = Some(result.clone());
            task.encode_into(&mut self.encode_buf);
            tasks.insert(id, self.encode_buf.as_slice())?;
        }
        let mut emit_ids = Vec::with_capacity(emits.len());
        if !emits.is_empty() {
            let mut next_emit = {
                let meta = write_txn.open_table(META_TABLE)?;
                meta.get(NEXT_EMIT_KEY)?.map(|g| g.value()).unwrap_or(0)
            };
            {
                let mut outbox = write_txn.open_table(OUTBOX_TABLE)?;
                for (target_topic, payload) in emits {
                    // Cloning a `Bytes` here is a refcount bump — no
                    // memcpy. The caller's slice survives until we
                    // commit the redb txn (encoded inline below).
                    let emit = PendingEmit {
                        emit_id: next_emit,
                        task_id: id,
                        target_topic: target_topic.to_string(),
                        payload: payload.clone(),
                    };
                    emit.encode_into(&mut self.encode_buf);
                    outbox.insert(next_emit, self.encode_buf.as_slice())?;
                    emit_ids.push(next_emit);
                    next_emit += 1;
                }
            }
            {
                let mut meta = write_txn.open_table(META_TABLE)?;
                meta.insert(NEXT_EMIT_KEY, next_emit)?;
            }
        }
        write_txn.commit()?;
        Ok(emit_ids)
    }

    /// Snapshot of every outbox entry awaiting delivery.
    pub fn pending_emits(&self) -> Result<Vec<PendingEmit>> {
        let read_txn = self.db.begin_read()?;
        let outbox = read_txn.open_table(OUTBOX_TABLE)?;
        let mut out = Vec::new();
        for entry in outbox.iter()? {
            let (k, v) = entry?;
            out.push(PendingEmit::decode(k.value(), v.value())?);
        }
        Ok(out)
    }

    /// Remove an outbox entry. Called by the dispatcher once the topic
    /// write is confirmed.
    pub fn mark_emit_delivered(&mut self, emit_id: u64) -> Result<bool> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut outbox = write_txn.open_table(OUTBOX_TABLE)?;
            outbox.remove(emit_id)?.is_some()
        };
        write_txn.commit()?;
        Ok(removed)
    }

    pub fn fail(&mut self, id: TaskId, error: String) -> Result<()> {
        self.fail_bytes(id, bytes::Bytes::from(error.into_bytes()))
    }

    /// `Bytes`-accepting variant of [`Self::fail`]. Skips the
    /// `String → Bytes` move when the caller already holds a
    /// refcounted error buffer.
    pub fn fail_bytes(&mut self, id: TaskId, error: bytes::Bytes) -> Result<()> {
        self.update(id, |task| {
            if task.status.is_terminal() {
                Err(WorkflowError::TerminalTask { id })
            } else {
                task.status = TaskStatus::Failed;
                task.error = Some(error.clone());
                Ok(())
            }
        })
    }

    pub fn cancel(&mut self, id: TaskId) -> Result<usize> {
        let mut to_cancel = vec![id];
        let mut frontier = 0;
        while frontier < to_cancel.len() {
            let current = to_cancel[frontier];
            let kids = self.children(current)?;
            // Pre-check: refuse BEFORE growing `to_cancel`, so a
            // single oversized `children(current)` response can't
            // push the set past the cap. Honours
            // `EngineOptions::cancel_max_descendants` — 0 means
            // unlimited.
            if self.cancel_max_descendants > 0
                && to_cancel.len() + kids.len() > self.cancel_max_descendants
            {
                return Err(WorkflowError::CancelFanoutExceeded {
                    limit: self.cancel_max_descendants,
                });
            }
            to_cancel.extend(kids);
            frontier += 1;
        }

        let mut cancelled = 0;
        let write_txn = self.db.begin_write()?;
        {
            let mut tasks = write_txn.open_table(TASKS_TABLE)?;
            for tid in &to_cancel {
                let buf = match tasks.get(*tid)? {
                    Some(g) => bytes::Bytes::from(g.value().to_vec()),
                    None => continue,
                };
                let mut t = Task::decode_from(*tid, buf)?;
                if t.status.is_terminal() {
                    continue;
                }
                t.status = TaskStatus::Cancelled;
                t.encode_into(&mut self.encode_buf);
                tasks.insert(*tid, self.encode_buf.as_slice())?;
                cancelled += 1;
            }
        }
        write_txn.commit()?;
        Ok(cancelled)
    }

    /// Aggregate task counts by status across the whole workflow.
    /// Cheap because we only inspect the first byte of each encoded
    /// task — the status byte is the encoding's prefix, so a full
    /// `Task::decode` is unnecessary. One read transaction, no
    /// allocations beyond the returned struct.
    pub fn stats(&self) -> Result<EngineStats> {
        let read_txn = self.db.begin_read()?;
        let tasks = read_txn.open_table(TASKS_TABLE)?;
        let mut stats = EngineStats::default();
        for entry in tasks.iter()? {
            let (_, v) = entry?;
            stats.total += 1;
            let bytes = v.value();
            if let Some(first) = bytes.first() {
                match TaskStatus::from_u8(*first) {
                    Ok(TaskStatus::Pending) => stats.pending += 1,
                    Ok(TaskStatus::Running) => stats.running += 1,
                    Ok(TaskStatus::Done) => stats.done += 1,
                    Ok(TaskStatus::Failed) => stats.failed += 1,
                    Ok(TaskStatus::Cancelled) => stats.cancelled += 1,
                    Err(_) => {}
                }
            }
        }
        Ok(stats)
    }

    pub fn get(&self, id: TaskId) -> Result<Option<Task>> {
        let read_txn = self.db.begin_read()?;
        let tasks = read_txn.open_table(TASKS_TABLE)?;
        match tasks.get(id)? {
            Some(g) => Ok(Some(Task::decode_from(
                id,
                bytes::Bytes::from(g.value().to_vec()),
            )?)),
            None => Ok(None),
        }
    }

    pub fn children(&self, parent_id: TaskId) -> Result<Vec<TaskId>> {
        let read_txn = self.db.begin_read()?;
        let children = read_txn.open_multimap_table(CHILDREN_TABLE)?;
        let mut out = Vec::new();
        for entry in children.get(parent_id)? {
            out.push(entry?.value());
        }
        Ok(out)
    }

    fn create(&mut self, parent_id: Option<TaskId>, payload: &[u8]) -> Result<TaskId> {
        let write_txn = self.db.begin_write()?;
        let id = {
            let mut meta = write_txn.open_table(META_TABLE)?;
            let current = meta.get(NEXT_ID_KEY)?.map(|g| g.value()).unwrap_or(0);
            meta.insert(NEXT_ID_KEY, current + 1)?;
            current
        };
        {
            let mut tasks = write_txn.open_table(TASKS_TABLE)?;
            let task = Task {
                id,
                parent_id,
                status: TaskStatus::Pending,
                created_nanos: now_nanos(),
                payload: bytes::Bytes::copy_from_slice(payload),
                result: None,
                error: None,
            };
            task.encode_into(&mut self.encode_buf);
            tasks.insert(id, self.encode_buf.as_slice())?;
            if let Some(pid) = parent_id {
                let mut children = write_txn.open_multimap_table(CHILDREN_TABLE)?;
                children.insert(pid, id)?;
            }
        }
        write_txn.commit()?;
        Ok(id)
    }

    fn update<F>(&mut self, id: TaskId, mutate: F) -> Result<()>
    where
        F: FnOnce(&mut Task) -> Result<()>,
    {
        let write_txn = self.db.begin_write()?;
        {
            let mut tasks = write_txn.open_table(TASKS_TABLE)?;
            let buf = bytes::Bytes::from(
                tasks
                    .get(id)?
                    .ok_or(WorkflowError::TaskNotFound { id })?
                    .value()
                    .to_vec(),
            );
            let mut task = Task::decode_from(id, buf)?;
            mutate(&mut task)?;
            task.encode_into(&mut self.encode_buf);
            tasks.insert(id, self.encode_buf.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

fn ensure_schema(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    txn.open_table(TASKS_TABLE)?;
    txn.open_multimap_table(CHILDREN_TABLE)?;
    txn.open_table(META_TABLE)?;
    txn.open_table(DEDUP_TABLE)?;
    txn.open_table(OUTBOX_TABLE)?;
    txn.commit()?;
    Ok(())
}

/// One emit pending delivery from a workflow to a topic. Stored in the
/// outbox until the broker-side dispatcher confirms the topic write.
#[derive(Debug, Clone)]
pub struct PendingEmit {
    pub emit_id: u64,
    pub task_id: TaskId,
    pub target_topic: String,
    /// Refcounted slice. Wrapped as `Bytes` so the outbox dispatcher
    /// can hand it to the topic's `send_*` API without re-buffering.
    pub payload: bytes::Bytes,
}

impl PendingEmit {
    /// Encode into a caller-provided buffer, reusing its allocation
    /// across emits. The engine's `complete_with_emits` keeps one
    /// `encode_buf` and clears it between iterations so a fan-out of
    /// N emits costs O(1) Vec growth and not N fresh allocations.
    fn encode_into(&self, buf: &mut Vec<u8>) {
        let needed = 8 + 4 + self.target_topic.len() + 4 + self.payload.len();
        buf.clear();
        buf.reserve(needed);
        buf.extend_from_slice(&self.task_id.to_le_bytes());
        buf.extend_from_slice(&(self.target_topic.len() as u32).to_le_bytes());
        buf.extend_from_slice(self.target_topic.as_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);
    }

    fn decode(emit_id: u64, bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 + 4 {
            return Err(WorkflowError::CorruptTask);
        }
        let task_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let mut c = 8usize;
        let topic_len = u32::from_le_bytes(bytes[c..c + 4].try_into().unwrap()) as usize;
        c += 4;
        if bytes.len() < c + topic_len + 4 {
            return Err(WorkflowError::CorruptTask);
        }
        let target_topic = String::from_utf8(bytes[c..c + topic_len].to_vec())
            .map_err(|_| WorkflowError::CorruptTask)?;
        c += topic_len;
        let payload_len = u32::from_le_bytes(bytes[c..c + 4].try_into().unwrap()) as usize;
        c += 4;
        if bytes.len() < c + payload_len {
            return Err(WorkflowError::CorruptTask);
        }
        let payload = bytes::Bytes::copy_from_slice(&bytes[c..c + payload_len]);
        Ok(Self { emit_id, task_id, target_topic, payload })
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
