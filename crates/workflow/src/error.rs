use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("state db: {0}")]
    StateDb(Box<dyn std::error::Error + Send + Sync>),

    #[error("task {id} not found")]
    TaskNotFound { id: u64 },

    #[error("task {id} is in terminal state and cannot be updated")]
    TerminalTask { id: u64 },

    #[error("corrupt task record")]
    CorruptTask,
}

macro_rules! redb_from {
    ($t:ty) => {
        impl From<$t> for WorkflowError {
            fn from(e: $t) -> Self {
                WorkflowError::StateDb(Box::new(e))
            }
        }
    };
}

redb_from!(redb::DatabaseError);
redb_from!(redb::TransactionError);
redb_from!(redb::TableError);
redb_from!(redb::CommitError);
redb_from!(redb::StorageError);

pub type Result<T> = std::result::Result<T, WorkflowError>;
