use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("checksum mismatch at offset {offset}")]
    Corrupt { offset: u64 },

    #[error("unexpected end of file at offset {offset}")]
    UnexpectedEof { offset: u64 },

    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("record {id} not found")]
    RecordNotFound { id: u64 },

    #[error("invalid segment file name: {name}")]
    InvalidSegmentName { name: String },

    #[error("segment gap: expected base record id {expected}, found {found}")]
    SegmentGap { expected: u64, found: u64 },
}

pub type Result<T> = std::result::Result<T, StorageError>;
