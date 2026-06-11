//! Task value type and binary encoding.
//!
//! Encoding (little-endian):
//!   [u8 status]
//!   [u8 parent_present]
//!   [u64 parent_id]              (8 bytes always; ignored if !parent_present)
//!   [u64 created_nanos]
//!   [u32 payload_len][N payload]
//!   [u32 result_len][N result]   (0 = no result)
//!   [u32 error_len][N error]     (0 = no error)

use crate::error::{Result, WorkflowError};

pub type TaskId = u64;

const HEADER_SIZE: usize = 1 + 1 + 8 + 8 + 4 + 4 + 4;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Pending = 0,
    Running = 1,
    Done = 2,
    Failed = 3,
    Cancelled = 4,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }

    pub fn from_u8(b: u8) -> Result<Self> {
        match b {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Done),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Cancelled),
            _ => Err(WorkflowError::CorruptTask),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: TaskId,
    pub parent_id: Option<TaskId>,
    pub status: TaskStatus,
    pub created_nanos: u64,
    /// Refcounted slice. On decode the bytes are wrapped via
    /// `Bytes::copy_from_slice` (a single alloc the redb-deserialise
    /// path was already paying); on the read-back path the broker
    /// hands these straight to prost without re-buffering.
    pub payload: bytes::Bytes,
    pub result: Option<bytes::Bytes>,
    pub error: Option<String>,
}

impl Task {
    /// Encode into a caller-provided buffer, reusing its allocation.
    /// The engine pools one `encode_buf` across every task write in a
    /// transaction so a long completion loop doesn't allocate a
    /// fresh `Vec` per task. The buffer is cleared first so callers
    /// can pass a recycled scratch.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        let result_bytes: &[u8] = self.result.as_deref().unwrap_or(&[]);
        let error_bytes: &[u8] = self.error.as_deref().map(str::as_bytes).unwrap_or(b"");
        let needed =
            HEADER_SIZE + self.payload.len() + result_bytes.len() + error_bytes.len();
        buf.clear();
        buf.reserve(needed);
        buf.push(self.status as u8);
        buf.push(u8::from(self.parent_id.is_some()));
        buf.extend_from_slice(&self.parent_id.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&self.created_nanos.to_le_bytes());

        buf.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.payload);

        buf.extend_from_slice(&(result_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(result_bytes);

        buf.extend_from_slice(&(error_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(error_bytes);
    }

    /// Compatibility wrapper that allocates a fresh `Vec` and
    /// delegates to [`Self::encode_into`]. Callers that aren't in a
    /// hot loop can keep using this; the engine's transaction paths
    /// switched to `encode_into` with a pooled buffer.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf);
        buf
    }

    /// Compatibility wrapper for callers that still hand decode a
    /// borrowed slice. Allocates once internally via `to_vec()` and
    /// delegates to [`Self::decode_from`] so the `payload` and
    /// `result` fields share that single allocation instead of each
    /// paying their own `Bytes::copy_from_slice`.
    pub fn decode(id: TaskId, bytes: &[u8]) -> Result<Self> {
        Self::decode_from(id, bytes::Bytes::from(bytes.to_vec()))
    }

    /// Take ownership of the encoded buffer (typically a redb
    /// `AccessGuard::value().to_vec()` wrapped as `Bytes`) and slice
    /// `payload` and `result` views out of it via refcount bumps.
    /// One heap allocation per Task decode, regardless of how many
    /// `Bytes` fields the encoding carries.
    pub fn decode_from(id: TaskId, buf: bytes::Bytes) -> Result<Self> {
        let bytes: &[u8] = &buf;
        if bytes.len() < HEADER_SIZE {
            return Err(WorkflowError::CorruptTask);
        }
        let status = TaskStatus::from_u8(bytes[0])?;
        let has_parent = bytes[1] != 0;
        let parent_raw = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let parent_id = if has_parent { Some(parent_raw) } else { None };
        let created_nanos = u64::from_le_bytes(bytes[10..18].try_into().unwrap());

        let mut cursor = 18usize;

        let payload_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if bytes.len() < cursor + payload_len {
            return Err(WorkflowError::CorruptTask);
        }
        // `Bytes::slice` is a refcount bump into the same allocation
        // as `buf` — no realloc, no memcpy.
        let payload = buf.slice(cursor..cursor + payload_len);
        cursor += payload_len;

        if bytes.len() < cursor + 4 {
            return Err(WorkflowError::CorruptTask);
        }
        let result_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if bytes.len() < cursor + result_len {
            return Err(WorkflowError::CorruptTask);
        }
        let result = if result_len > 0 {
            Some(buf.slice(cursor..cursor + result_len))
        } else {
            None
        };
        cursor += result_len;

        if bytes.len() < cursor + 4 {
            return Err(WorkflowError::CorruptTask);
        }
        let error_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if bytes.len() < cursor + error_len {
            return Err(WorkflowError::CorruptTask);
        }
        let error = if error_len > 0 {
            Some(
                String::from_utf8(bytes[cursor..cursor + error_len].to_vec())
                    .map_err(|_| WorkflowError::CorruptTask)?,
            )
        } else {
            None
        };

        Ok(Self {
            id,
            parent_id,
            status,
            created_nanos,
            payload,
            result,
            error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_root_round_trips() {
        let t = Task {
            id: 0,
            parent_id: None,
            status: TaskStatus::Pending,
            created_nanos: 12345,
            payload: bytes::Bytes::from_static(b"work"),
            result: None,
            error: None,
        };
        let bytes = t.encode();
        let decoded = Task::decode(0, &bytes).unwrap();
        assert!(decoded.parent_id.is_none());
        assert_eq!(decoded.status, TaskStatus::Pending);
        assert_eq!(&decoded.payload[..], b"work");
        assert!(decoded.result.is_none());
        assert!(decoded.error.is_none());
    }

    #[test]
    fn done_with_result_round_trips() {
        let t = Task {
            id: 5,
            parent_id: Some(2),
            status: TaskStatus::Done,
            created_nanos: 0,
            payload: bytes::Bytes::from_static(b"in"),
            result: Some(bytes::Bytes::from_static(b"out")),
            error: None,
        };
        let bytes = t.encode();
        let decoded = Task::decode(5, &bytes).unwrap();
        assert_eq!(decoded.parent_id, Some(2));
        assert_eq!(decoded.result.as_deref(), Some(b"out".as_slice()));
    }

    #[test]
    fn failed_with_error_round_trips() {
        let t = Task {
            id: 7,
            parent_id: Some(1),
            status: TaskStatus::Failed,
            created_nanos: 0,
            payload: bytes::Bytes::new(),
            result: None,
            error: Some("boom".into()),
        };
        let bytes = t.encode();
        let decoded = Task::decode(7, &bytes).unwrap();
        assert_eq!(decoded.error.as_deref(), Some("boom"));
    }

    #[test]
    fn is_terminal_classifies_states() {
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
        assert!(TaskStatus::Done.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
    }
}
