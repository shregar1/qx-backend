//! ACL store + enforcement.
//!
//! Kafka ACLs are tuples of `(resource_type, resource_name, pattern_type,
//! principal, host, operation, permission_type)`. We mirror that shape
//! exactly so the wire APIs (DescribeAcls / CreateAcls / DeleteAcls) can
//! round-trip cleanly. Storage is in-memory today, refreshable via the
//! CreateAcls / DeleteAcls request handlers; persisting to redb is a
//! small extension once the in-memory shape stabilizes.
//!
//! Enforcement happens once per request in the Kafka dispatcher:
//! `authorize(state, action)` returns `Allowed` or `Denied`. We default
//! to "allow when no ACLs are configured at all" so the listener stays
//! open for development. As soon as one ACL is created, the default
//! flips to deny.

use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Topic = 2,
    Group = 3,
    Cluster = 4,
    TransactionalId = 5,
}

impl ResourceType {
    pub fn from_kafka(v: i8) -> Option<Self> {
        match v {
            2 => Some(Self::Topic),
            3 => Some(Self::Group),
            4 => Some(Self::Cluster),
            5 => Some(Self::TransactionalId),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternType {
    Literal = 3,
    Prefixed = 4,
}

impl PatternType {
    pub fn from_kafka(v: i8) -> Option<Self> {
        match v {
            3 => Some(Self::Literal),
            4 => Some(Self::Prefixed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
}

impl Operation {
    pub fn from_kafka(v: i8) -> Option<Self> {
        match v {
            3 => Some(Self::Read),
            4 => Some(Self::Write),
            5 => Some(Self::Create),
            6 => Some(Self::Delete),
            7 => Some(Self::Alter),
            8 => Some(Self::Describe),
            9 => Some(Self::ClusterAction),
            10 => Some(Self::DescribeConfigs),
            11 => Some(Self::AlterConfigs),
            12 => Some(Self::IdempotentWrite),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    Deny = 2,
    Allow = 3,
}

impl Permission {
    pub fn from_kafka(v: i8) -> Option<Self> {
        match v {
            2 => Some(Self::Deny),
            3 => Some(Self::Allow),
            _ => None,
        }
    }
}

/// One ACL row. Wildcards: principal `"User:*"` matches any principal;
/// host `"*"` matches any host; resource_name `"*"` (literal pattern)
/// matches any resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,
    pub pattern_type: PatternType,
    pub principal: String,
    pub host: String,
    pub operation: Operation,
    pub permission: Permission,
}

#[derive(Debug, Default)]
pub struct AclStore {
    entries: Mutex<HashSet<AclEntry>>,
}

impl AclStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }

    pub fn add(&self, entry: AclEntry) {
        self.entries.lock().unwrap().insert(entry);
    }

    /// Delete every entry that matches the supplied filter. Returns the
    /// removed entries so the wire response can echo them.
    pub fn delete_matching(&self, filter: AclFilter) -> Vec<AclEntry> {
        let mut entries = self.entries.lock().unwrap();
        let matches: Vec<AclEntry> =
            entries.iter().filter(|e| filter.matches(e)).cloned().collect();
        for m in &matches {
            entries.remove(m);
        }
        matches
    }

    /// Snapshot every entry the filter matches (for DescribeAcls).
    pub fn describe(&self, filter: AclFilter) -> Vec<AclEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| filter.matches(e))
            .cloned()
            .collect()
    }

    /// Authorize an action by the rules:
    ///   1. Any matching DENY wins.
    ///   2. Otherwise, an ALLOW match permits.
    ///   3. Otherwise, deny.
    ///
    /// "Empty store" is a special case — we allow everything so an
    /// out-of-the-box broker isn't immediately locked out. The moment
    /// one ACL is created, the default flips to deny.
    pub fn authorize(
        &self,
        principal: &str,
        resource_type: ResourceType,
        resource_name: &str,
        operation: Operation,
    ) -> bool {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return true;
        }
        let mut found_allow = false;
        for entry in entries.iter() {
            if entry.resource_type != resource_type {
                continue;
            }
            if !resource_matches(entry, resource_name) {
                continue;
            }
            if !principal_matches(&entry.principal, principal) {
                continue;
            }
            if entry.operation != operation && entry.operation != Operation::Alter {
                // Alter implicitly subsumes most write-side ops in Kafka.
                continue;
            }
            match entry.permission {
                Permission::Deny => return false,
                Permission::Allow => found_allow = true,
            }
        }
        found_allow
    }
}

fn resource_matches(entry: &AclEntry, resource_name: &str) -> bool {
    match entry.pattern_type {
        PatternType::Literal => entry.resource_name == "*" || entry.resource_name == resource_name,
        PatternType::Prefixed => resource_name.starts_with(&entry.resource_name),
    }
}

fn principal_matches(rule: &str, requested: &str) -> bool {
    rule == "User:*" || rule == requested
}

/// Filter shape used by DescribeAcls and DeleteAcls. Empty / None fields
/// mean "match everything".
#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<Operation>,
    pub permission: Option<Permission>,
}

impl AclFilter {
    fn matches(&self, e: &AclEntry) -> bool {
        if let Some(t) = self.resource_type {
            if t != e.resource_type {
                return false;
            }
        }
        if let Some(name) = &self.resource_name {
            if name != &e.resource_name {
                return false;
            }
        }
        if let Some(p) = self.pattern_type {
            if p != e.pattern_type {
                return false;
            }
        }
        if let Some(p) = &self.principal {
            if p != &e.principal {
                return false;
            }
        }
        if let Some(h) = &self.host {
            if h != &e.host {
                return false;
            }
        }
        if let Some(op) = self.operation {
            if op != e.operation {
                return false;
            }
        }
        if let Some(p) = self.permission {
            if p != e.permission {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_store_allows_everything() {
        let store = AclStore::new();
        assert!(store.authorize("User:alice", ResourceType::Topic, "events", Operation::Write));
    }

    #[test]
    fn deny_wins_over_allow() {
        let store = AclStore::new();
        store.add(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "*".into(),
            pattern_type: PatternType::Literal,
            principal: "User:*".into(),
            host: "*".into(),
            operation: Operation::Read,
            permission: Permission::Allow,
        });
        store.add(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "secret".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: Operation::Read,
            permission: Permission::Deny,
        });
        assert!(store.authorize("User:bob", ResourceType::Topic, "secret", Operation::Read));
        assert!(!store.authorize("User:alice", ResourceType::Topic, "secret", Operation::Read));
    }

    #[test]
    fn prefix_pattern() {
        let store = AclStore::new();
        store.add(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "prod-".into(),
            pattern_type: PatternType::Prefixed,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: Operation::Write,
            permission: Permission::Allow,
        });
        assert!(store.authorize("User:alice", ResourceType::Topic, "prod-orders", Operation::Write));
        assert!(!store.authorize("User:alice", ResourceType::Topic, "dev-orders", Operation::Write));
    }
}
