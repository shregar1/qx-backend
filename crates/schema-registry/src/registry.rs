//! Schema registry implementation. Two redb tables back the registry:
//!   `schemas: u64 (schema_id) -> encoded Schema`
//!   `meta:    &str -> &[u8]`              (next_schema_id, …)
//!
//! Per-subject version lists are encoded inline in a third table keyed by
//! the subject name; values are length-prefixed lists of schema ids.

use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};

use crate::error::{RegistryError, Result};

pub type SchemaId = u64;
pub type Version = u32;

const SCHEMAS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("schemas");
const SUBJECTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("subjects");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("meta");
const META_NEXT_ID: &str = "next_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub id: SchemaId,
    pub subject: String,
    pub version: Version,
    pub bytes: Vec<u8>,
}

/// Compatibility policy. For now compatibility checks always allow new
/// schemas; users wanting BACKWARD / FORWARD / FULL semantics plug in their
/// own check via a future callback API.
#[derive(Debug, Clone, Copy, Default)]
pub enum Compatibility {
    #[default]
    Allow,
}

pub struct Registry {
    db: Database,
}

impl Registry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path)?;
        ensure_schema(&db)?;
        Ok(Self { db })
    }

    /// Register a new schema under `subject`. The subject's next version is
    /// allocated and the schema gets a globally-unique id.
    pub fn register(&self, subject: &str, bytes: &[u8]) -> Result<Schema> {
        let write_txn = self.db.begin_write()?;
        let schema = {
            let mut meta = write_txn.open_table(META_TABLE)?;
            let next = meta
                .get(META_NEXT_ID)?
                .map(|g| g.value())
                .unwrap_or(0);
            meta.insert(META_NEXT_ID, next + 1)?;

            let mut subjects = write_txn.open_table(SUBJECTS_TABLE)?;
            let existing_ids = subjects
                .get(subject)?
                .map(|g| decode_id_list(g.value()))
                .transpose()?
                .unwrap_or_default();
            let version = (existing_ids.len() as Version) + 1;

            let mut new_ids = existing_ids;
            new_ids.push(next);
            let encoded_ids = encode_id_list(&new_ids);
            subjects.insert(subject, encoded_ids.as_slice())?;

            let schema = Schema {
                id: next,
                subject: subject.to_string(),
                version,
                bytes: bytes.to_vec(),
            };
            let mut schemas = write_txn.open_table(SCHEMAS_TABLE)?;
            schemas.insert(next, encode_schema(&schema).as_slice())?;
            schema
        };
        write_txn.commit()?;
        Ok(schema)
    }

    pub fn get_by_id(&self, id: SchemaId) -> Result<Schema> {
        let read_txn = self.db.begin_read()?;
        let schemas = read_txn.open_table(SCHEMAS_TABLE)?;
        let bytes = schemas
            .get(id)?
            .ok_or(RegistryError::SchemaIdNotFound(id))?
            .value()
            .to_vec();
        decode_schema(&bytes)
    }

    pub fn get_version(&self, subject: &str, version: Version) -> Result<Schema> {
        let ids = self.list_ids(subject)?;
        let idx = (version as usize)
            .checked_sub(1)
            .filter(|&i| i < ids.len())
            .ok_or_else(|| RegistryError::VersionNotFound {
                subject: subject.to_string(),
                version,
            })?;
        self.get_by_id(ids[idx])
    }

    pub fn list_versions(&self, subject: &str) -> Result<Vec<Version>> {
        let ids = self.list_ids(subject)?;
        Ok((1..=ids.len() as Version).collect())
    }

    pub fn get_latest(&self, subject: &str) -> Result<Schema> {
        let ids = self.list_ids(subject)?;
        let last = *ids
            .last()
            .ok_or_else(|| RegistryError::SubjectNotFound(subject.to_string()))?;
        self.get_by_id(last)
    }

    pub fn list_subjects(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let subjects = read_txn.open_table(SUBJECTS_TABLE)?;
        let mut out = Vec::new();
        for entry in subjects.iter()? {
            let (k, _) = entry?;
            out.push(k.value().to_string());
        }
        Ok(out)
    }

    fn list_ids(&self, subject: &str) -> Result<Vec<SchemaId>> {
        let read_txn = self.db.begin_read()?;
        let subjects = read_txn.open_table(SUBJECTS_TABLE)?;
        let g = subjects
            .get(subject)?
            .ok_or_else(|| RegistryError::SubjectNotFound(subject.to_string()))?;
        decode_id_list(g.value())
    }
}

fn ensure_schema(db: &Database) -> Result<()> {
    let txn = db.begin_write()?;
    txn.open_table(SCHEMAS_TABLE)?;
    txn.open_table(SUBJECTS_TABLE)?;
    txn.open_table(META_TABLE)?;
    txn.commit()?;
    Ok(())
}

fn encode_id_list(ids: &[SchemaId]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + ids.len() * 8);
    out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for id in ids {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out
}

fn decode_id_list(bytes: &[u8]) -> Result<Vec<SchemaId>> {
    if bytes.len() < 4 {
        return Err(RegistryError::CorruptRecord);
    }
    let n = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if bytes.len() < 4 + n * 8 {
        return Err(RegistryError::CorruptRecord);
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let start = 4 + i * 8;
        out.push(u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap()));
    }
    Ok(out)
}

fn encode_schema(s: &Schema) -> Vec<u8> {
    let subj = s.subject.as_bytes();
    let mut out = Vec::with_capacity(8 + 4 + 4 + subj.len() + 4 + s.bytes.len());
    out.extend_from_slice(&s.id.to_le_bytes());
    out.extend_from_slice(&s.version.to_le_bytes());
    out.extend_from_slice(&(subj.len() as u32).to_le_bytes());
    out.extend_from_slice(subj);
    out.extend_from_slice(&(s.bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&s.bytes);
    out
}

fn decode_schema(bytes: &[u8]) -> Result<Schema> {
    if bytes.len() < 16 {
        return Err(RegistryError::CorruptRecord);
    }
    let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let subj_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if bytes.len() < 16 + subj_len + 4 {
        return Err(RegistryError::CorruptRecord);
    }
    let subject = std::str::from_utf8(&bytes[16..16 + subj_len])
        .map_err(|_| RegistryError::CorruptRecord)?
        .to_string();
    let payload_start = 16 + subj_len;
    let payload_len = u32::from_le_bytes(
        bytes[payload_start..payload_start + 4].try_into().unwrap(),
    ) as usize;
    let payload_bytes_start = payload_start + 4;
    if bytes.len() < payload_bytes_start + payload_len {
        return Err(RegistryError::CorruptRecord);
    }
    let payload = bytes[payload_bytes_start..payload_bytes_start + payload_len].to_vec();
    Ok(Schema {
        id,
        subject,
        version,
        bytes: payload,
    })
}
