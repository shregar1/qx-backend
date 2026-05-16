use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("storage: {0}")]
    Storage(#[from] qx_storage::StorageError),

    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("state db: {0}")]
    StateDb(Box<dyn std::error::Error + Send + Sync>),

    #[error("unknown receipt: record {record_id}")]
    UnknownReceipt { record_id: u64 },

    #[error("stale receipt: visibility timeout elapsed or message reclaimed")]
    StaleReceipt,

    #[error("operation requires message to be in-flight")]
    NotInFlight,

    #[error("corrupt state record")]
    CorruptState,
}

macro_rules! redb_from {
    ($t:ty) => {
        impl From<$t> for QueueError {
            fn from(e: $t) -> Self {
                QueueError::StateDb(Box::new(e))
            }
        }
    };
}

redb_from!(redb::DatabaseError);
redb_from!(redb::TransactionError);
redb_from!(redb::TableError);
redb_from!(redb::CommitError);
redb_from!(redb::StorageError);

pub type Result<T> = std::result::Result<T, QueueError>;
