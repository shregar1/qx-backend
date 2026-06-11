//! Stub handlers for admin APIs we acknowledge but don't model:
//! client quotas (48/49), SCRAM credentials (50/51), partition
//! reassignments (45/46).
//!
//! The ACL handlers (29/30/31) live alongside but are *real* — they
//! mutate a `Broker::acls()` store that the dispatcher consults on
//! every API call. See `acls.rs` for the enforcement rules.

use super::acls::{AclEntry, AclFilter, Operation, PatternType, Permission, ResourceType};
use super::codec::{Reader, WireResult, Writer};
use super::error_codes;
use crate::server::Broker;

// ─────────────────────── DescribeAcls (29) v3 ──────────────────────────

pub const DESCRIBE_ACLS_KEY: i16 = 29;
pub const DESCRIBE_ACLS_MIN: i16 = 3;
pub const DESCRIBE_ACLS_MAX: i16 = 3;

pub fn handle_describe_acls(w: &mut Writer, broker: &Broker, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let resource_type = r.read_i8()?;
    let resource_name = r.read_compact_nullable_string()?;
    let pattern_type = r.read_i8()?;
    let principal = r.read_compact_nullable_string()?;
    let host = r.read_compact_nullable_string()?;
    let operation = r.read_i8()?;
    let permission = r.read_i8()?;
    r.read_tagged_fields()?;

    let filter = AclFilter {
        resource_type: ResourceType::from_kafka(resource_type),
        resource_name,
        pattern_type: PatternType::from_kafka(pattern_type),
        principal,
        host,
        operation: Operation::from_kafka(operation),
        permission: Permission::from_kafka(permission),
    };

    let entries = broker.acls().describe(filter);
    // Group by (resource_type, resource_name, pattern_type) for the wire shape.
    let mut grouped: std::collections::BTreeMap<(i8, String, i8), Vec<AclEntry>> =
        std::collections::BTreeMap::new();
    for e in entries {
        grouped
            .entry((e.resource_type as i8, e.resource_name.clone(), e.pattern_type as i8))
            .or_default()
            .push(e);
    }

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_compact_array_len(grouped.len() as u32);
    for ((res_type, res_name, pat_type), aces) in grouped {
        w.write_i8(res_type);
        w.write_compact_string(&res_name);
        w.write_i8(pat_type);
        w.write_compact_array_len(aces.len() as u32);
        for ace in aces {
            w.write_compact_string(&ace.principal);
            w.write_compact_string(&ace.host);
            w.write_i8(ace.operation as i8);
            w.write_i8(ace.permission as i8);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────── CreateAcls (30) v3 ────────────────────────────

pub const CREATE_ACLS_KEY: i16 = 30;
pub const CREATE_ACLS_MIN: i16 = 3;
pub const CREATE_ACLS_MAX: i16 = 3;

pub fn handle_create_acls(w: &mut Writer, broker: &Broker, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    let mut requested: Vec<Result<AclEntry, &'static str>> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let resource_type = r.read_i8()?;
        let resource_name = r.read_compact_string()?;
        let pattern_type = r.read_i8()?;
        let principal = r.read_compact_string()?;
        let host = r.read_compact_string()?;
        let operation = r.read_i8()?;
        let permission = r.read_i8()?;
        r.read_tagged_fields()?;

        let entry = match (
            ResourceType::from_kafka(resource_type),
            PatternType::from_kafka(pattern_type),
            Operation::from_kafka(operation),
            Permission::from_kafka(permission),
        ) {
            (Some(rt), Some(pt), Some(op), Some(perm)) => Ok(AclEntry {
                resource_type: rt,
                resource_name,
                pattern_type: pt,
                principal,
                host,
                operation: op,
                permission: perm,
            }),
            _ => Err("invalid enum value in ACL"),
        };
        requested.push(entry);
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(count);
    for entry in requested {
        match entry {
            Ok(ace) => {
                broker.acls().add(ace);
                w.write_i16(error_codes::NONE);
                w.write_compact_nullable_string(None);
            }
            Err(msg) => {
                w.write_i16(error_codes::INVALID_REQUEST);
                w.write_compact_nullable_string(Some(msg));
            }
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────────── DeleteAcls (31) v3 ────────────────────────────

pub const DELETE_ACLS_KEY: i16 = 31;
pub const DELETE_ACLS_MIN: i16 = 3;
pub const DELETE_ACLS_MAX: i16 = 3;

pub fn handle_delete_acls(w: &mut Writer, broker: &Broker, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    let mut filters: Vec<AclFilter> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let resource_type = r.read_i8()?;
        let resource_name = r.read_compact_nullable_string()?;
        let pattern_type = r.read_i8()?;
        let principal = r.read_compact_nullable_string()?;
        let host = r.read_compact_nullable_string()?;
        let operation = r.read_i8()?;
        let permission = r.read_i8()?;
        r.read_tagged_fields()?;
        filters.push(AclFilter {
            resource_type: ResourceType::from_kafka(resource_type),
            resource_name,
            pattern_type: PatternType::from_kafka(pattern_type),
            principal,
            host,
            operation: Operation::from_kafka(operation),
            permission: Permission::from_kafka(permission),
        });
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(filters.len() as u32);
    for filter in filters {
        let matched = broker.acls().delete_matching(filter);
        w.write_i16(error_codes::NONE);
        w.write_compact_nullable_string(None);
        w.write_compact_array_len(matched.len() as u32);
        for ace in matched {
            w.write_i16(error_codes::NONE);
            w.write_compact_nullable_string(None);
            w.write_i8(ace.resource_type as i8);
            w.write_compact_string(&ace.resource_name);
            w.write_i8(ace.pattern_type as i8);
            w.write_compact_string(&ace.principal);
            w.write_compact_string(&ace.host);
            w.write_i8(ace.operation as i8);
            w.write_i8(ace.permission as i8);
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────────── DescribeClientQuotas (48) v1 ──────────────────────

pub const DESCRIBE_CLIENT_QUOTAS_KEY: i16 = 48;
pub const DESCRIBE_CLIENT_QUOTAS_MIN: i16 = 1;
pub const DESCRIBE_CLIENT_QUOTAS_MAX: i16 = 1;

pub fn handle_describe_client_quotas(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    for _ in 0..count {
        let _entity_type = r.read_compact_string()?;
        let _match_type = r.read_i8()?;
        let _match_value = r.read_compact_nullable_string()?;
        r.read_tagged_fields()?;
    }
    let _strict = r.read_bool()?;
    r.read_tagged_fields()?;

    w.write_i32(0);                          // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);   // error_message
    w.write_compact_array_len(0);            // entries: no quotas
    w.write_empty_tagged_fields();
    Ok(())
}

// ──────────────────── AlterClientQuotas (49) v1 ────────────────────────

pub const ALTER_CLIENT_QUOTAS_KEY: i16 = 49;
pub const ALTER_CLIENT_QUOTAS_MIN: i16 = 1;
pub const ALTER_CLIENT_QUOTAS_MAX: i16 = 1;

pub fn handle_alter_client_quotas(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let entry_count = if n == 0 { 0 } else { n - 1 };
    let mut entities_per_entry: Vec<Vec<(String, Option<String>)>> = Vec::with_capacity(entry_count as usize);
    for _ in 0..entry_count {
        let en = r.read_uvarint()?;
        let entity_count = if en == 0 { 0 } else { en - 1 };
        let mut entities = Vec::with_capacity(entity_count as usize);
        for _ in 0..entity_count {
            let et = r.read_compact_string()?;
            let ev = r.read_compact_nullable_string()?;
            r.read_tagged_fields()?;
            entities.push((et, ev));
        }
        let on = r.read_uvarint()?;
        let op_count = if on == 0 { 0 } else { on - 1 };
        for _ in 0..op_count {
            let _key = r.read_compact_string()?;
            let _value_bytes = r.read_raw(8, "AlterClientQuotas value (f64)")?;
            let _remove = r.read_bool()?;
            r.read_tagged_fields()?;
        }
        r.read_tagged_fields()?;
        entities_per_entry.push(entities);
    }
    let _validate_only = r.read_bool()?;
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_compact_array_len(entities_per_entry.len() as u32);
    for entities in entities_per_entry {
        w.write_i16(error_codes::NONE);
        w.write_compact_nullable_string(None);
        w.write_compact_array_len(entities.len() as u32);
        for (et, ev) in entities {
            w.write_compact_string(&et);
            w.write_compact_nullable_string(ev.as_deref());
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ──────────────── DescribeUserScramCredentials (50) v0 ─────────────────

pub const DESCRIBE_USER_SCRAM_KEY: i16 = 50;
pub const DESCRIBE_USER_SCRAM_MIN: i16 = 0;
pub const DESCRIBE_USER_SCRAM_MAX: i16 = 0;

pub fn handle_describe_user_scram(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let n = r.read_uvarint()?;
    let count = if n == 0 { 0 } else { n - 1 };
    for _ in 0..count {
        let _user = r.read_compact_string()?;
        r.read_tagged_fields()?;
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_compact_array_len(0); // results: no users
    w.write_empty_tagged_fields();
    Ok(())
}

// ────────────────── AlterUserScramCredentials (51) v0 ──────────────────

pub const ALTER_USER_SCRAM_KEY: i16 = 51;
pub const ALTER_USER_SCRAM_MIN: i16 = 0;
pub const ALTER_USER_SCRAM_MAX: i16 = 0;

pub fn handle_alter_user_scram(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let dn = r.read_uvarint()?;
    let del_count = if dn == 0 { 0 } else { dn - 1 };
    let mut deletions: Vec<String> = Vec::with_capacity(del_count as usize);
    for _ in 0..del_count {
        let user = r.read_compact_string()?;
        let _mechanism = r.read_i8()?;
        r.read_tagged_fields()?;
        deletions.push(user);
    }
    let un = r.read_uvarint()?;
    let upsert_count = if un == 0 { 0 } else { un - 1 };
    let mut upserts: Vec<String> = Vec::with_capacity(upsert_count as usize);
    for _ in 0..upsert_count {
        let user = r.read_compact_string()?;
        let _mechanism = r.read_i8()?;
        let _iterations = r.read_i32()?;
        let _salt = r.read_compact_bytes()?;
        let _salted_password = r.read_compact_bytes()?;
        r.read_tagged_fields()?;
        upserts.push(user);
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    let total = deletions.len() + upserts.len();
    w.write_compact_array_len(total as u32);
    for user in deletions.into_iter().chain(upserts.into_iter()) {
        w.write_i16(35); // UNSUPPORTED_VERSION — we have no SCRAM backend
        w.write_compact_nullable_string(Some("SCRAM credential storage not implemented"));
        w.write_compact_string(&user);
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────── AlterPartitionReassignments (45) v0 ───────────────────

pub const ALTER_PARTITION_REASSIGN_KEY: i16 = 45;
pub const ALTER_PARTITION_REASSIGN_MIN: i16 = 0;
pub const ALTER_PARTITION_REASSIGN_MAX: i16 = 0;

pub fn handle_alter_partition_reassign(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let _timeout_ms = r.read_i32()?;
    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    let mut layout: Vec<(String, Vec<i32>)> = Vec::with_capacity(topic_count as usize);
    for _ in 0..topic_count {
        let name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        let mut parts = Vec::with_capacity(pcount as usize);
        for _ in 0..pcount {
            let pidx = r.read_i32()?;
            // replicas — nullable compact array of i32
            let rn = r.read_uvarint()?;
            let rcount = if rn == 0 { 0 } else { rn - 1 };
            for _ in 0..rcount {
                let _replica = r.read_i32()?;
            }
            r.read_tagged_fields()?;
            parts.push(pidx);
        }
        r.read_tagged_fields()?;
        layout.push((name, parts));
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_compact_array_len(layout.len() as u32);
    for (name, parts) in layout {
        w.write_compact_string(&name);
        w.write_compact_array_len(parts.len() as u32);
        for p in parts {
            w.write_i32(p);
            // 56 = KAFKA_STORAGE_ERROR; we can't reassign on single broker.
            w.write_i16(56);
            w.write_compact_nullable_string(Some("single-broker — no reassignment possible"));
            w.write_empty_tagged_fields();
        }
        w.write_empty_tagged_fields();
    }
    w.write_empty_tagged_fields();
    Ok(())
}

// ─────────────── ListPartitionReassignments (46) v0 ────────────────────

pub const LIST_PARTITION_REASSIGN_KEY: i16 = 46;
pub const LIST_PARTITION_REASSIGN_MIN: i16 = 0;
pub const LIST_PARTITION_REASSIGN_MAX: i16 = 0;

pub fn handle_list_partition_reassign(w: &mut Writer, body: &[u8]) -> WireResult<()> {
    let mut r = Reader::new(body);
    let _timeout_ms = r.read_i32()?;
    let n = r.read_uvarint()?;
    let topic_count = if n == 0 { 0 } else { n - 1 };
    for _ in 0..topic_count {
        let _name = r.read_compact_string()?;
        let pn = r.read_uvarint()?;
        let pcount = if pn == 0 { 0 } else { pn - 1 };
        for _ in 0..pcount {
            let _pidx = r.read_i32()?;
        }
        r.read_tagged_fields()?;
    }
    r.read_tagged_fields()?;

    w.write_i32(0); // throttle_time_ms
    w.write_i16(error_codes::NONE);
    w.write_compact_nullable_string(None);
    w.write_compact_array_len(0); // no in-flight reassignments
    w.write_empty_tagged_fields();
    Ok(())
}
