use thiserror::Error;

#[derive(Debug, Error)]
pub enum RaftError {
    #[error("openraft initialise: {0}")]
    Initialise(String),

    #[error("openraft client write: {0}")]
    ClientWrite(String),

    #[error("openraft change_membership: {0}")]
    ChangeMembership(String),

    #[error("openraft initialization timeout (no leader elected)")]
    LeaderTimeout,

    #[error("unknown node: {0}")]
    UnknownNode(u64),

    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage: {0}")]
    Storage(#[from] qx_storage::StorageError),

    #[error("state db: {0}")]
    StateDb(Box<dyn std::error::Error + Send + Sync>),
}

macro_rules! redb_from {
    ($t:ty) => {
        impl From<$t> for RaftError {
            fn from(e: $t) -> Self {
                RaftError::StateDb(Box::new(e))
            }
        }
    };
}
redb_from!(redb::DatabaseError);
redb_from!(redb::TransactionError);
redb_from!(redb::TableError);
redb_from!(redb::CommitError);
redb_from!(redb::StorageError);

pub type Result<T> = std::result::Result<T, RaftError>;
