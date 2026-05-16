//! qx-schema-registry — subject-versioned schema storage.
//!
//! Schemas are stored as opaque bytes; the registry is format-agnostic
//! (Avro, JSON Schema, Protobuf descriptor — caller's choice). Each schema
//! gets a globally-unique `SchemaId` and a per-subject monotonically-incrementing
//! `Version`. Compatibility checking is a stub returning `Allow` for now —
//! production usage would compare schemas under a configured policy
//! (BACKWARD / FORWARD / FULL / NONE).

mod error;
mod registry;

pub use error::{RegistryError, Result};
pub use registry::{Compatibility, Registry, Schema, SchemaId, Version};
