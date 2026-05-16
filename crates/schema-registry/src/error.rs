use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("state db: {0}")]
    StateDb(Box<dyn std::error::Error + Send + Sync>),

    #[error("subject not found: {0}")]
    SubjectNotFound(String),

    #[error("schema id not found: {0}")]
    SchemaIdNotFound(u64),

    #[error("version not found: subject={subject} version={version}")]
    VersionNotFound { subject: String, version: u32 },

    #[error("incompatible schema for subject {subject}: {reason}")]
    Incompatible { subject: String, reason: String },

    #[error("corrupt registry record")]
    CorruptRecord,
}

macro_rules! redb_from {
    ($t:ty) => {
        impl From<$t> for RegistryError {
            fn from(e: $t) -> Self {
                RegistryError::StateDb(Box::new(e))
            }
        }
    };
}
redb_from!(redb::DatabaseError);
redb_from!(redb::TransactionError);
redb_from!(redb::TableError);
redb_from!(redb::CommitError);
redb_from!(redb::StorageError);

pub type Result<T> = std::result::Result<T, RegistryError>;
