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

pub struct Engine {
    db: Database,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        ensure_schema(&db)?;
        Ok(Self { db })
    }

    pub fn create_root(&mut self, payload: &[u8]) -> Result<TaskId> {
        self.create(None, payload)
    }

    pub fn create_child(&mut self, parent_id: TaskId, payload: &[u8]) -> Result<TaskId> {
        self.create(Some(parent_id), payload)
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

    pub fn complete(&mut self, id: TaskId, result: Vec<u8>) -> Result<()> {
        self.update(id, |task| {
            if task.status.is_terminal() {
                Err(WorkflowError::TerminalTask { id })
            } else {
                task.status = TaskStatus::Done;
                task.result = Some(result);
                Ok(())
            }
        })
    }

    pub fn fail(&mut self, id: TaskId, error: String) -> Result<()> {
        self.update(id, |task| {
            if task.status.is_terminal() {
                Err(WorkflowError::TerminalTask { id })
            } else {
                task.status = TaskStatus::Failed;
                task.error = Some(error);
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
            to_cancel.extend(kids);
            frontier += 1;
        }

        let mut cancelled = 0;
        let write_txn = self.db.begin_write()?;
        {
            let mut tasks = write_txn.open_table(TASKS_TABLE)?;
            for tid in &to_cancel {
                let bytes = match tasks.get(*tid)? {
                    Some(g) => g.value().to_vec(),
                    None => continue,
                };
                let mut t = Task::decode(*tid, &bytes)?;
                if t.status.is_terminal() {
                    continue;
                }
                t.status = TaskStatus::Cancelled;
                tasks.insert(*tid, t.encode().as_slice())?;
                cancelled += 1;
            }
        }
        write_txn.commit()?;
        Ok(cancelled)
    }

    pub fn get(&self, id: TaskId) -> Result<Option<Task>> {
        let read_txn = self.db.begin_read()?;
        let tasks = read_txn.open_table(TASKS_TABLE)?;
        match tasks.get(id)? {
            Some(g) => Ok(Some(Task::decode(id, g.value())?)),
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
                payload: payload.to_vec(),
                result: None,
                error: None,
            };
            tasks.insert(id, task.encode().as_slice())?;
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
            let bytes = tasks
                .get(id)?
                .ok_or(WorkflowError::TaskNotFound { id })?
                .value()
                .to_vec();
            let mut task = Task::decode(id, &bytes)?;
            mutate(&mut task)?;
            tasks.insert(id, task.encode().as_slice())?;
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
    txn.commit()?;
    Ok(())
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
