//! In-memory group coordinator: tracks members of each `(topic, group)` and
//! their partition assignments. Round-robin partition distribution; rebalance
//! on every `join` and `leave` (and lazily on heartbeat when a member is
//! discovered to have expired).
//!
//! State is volatile across broker restarts — v0 caveat. Persistent group
//! state is a separate feature (Kafka stores it in a special compacted topic).

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub type MemberId = u64;

#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub last_heartbeat_nanos: u64,
    pub assignment: Vec<u32>,
}

#[derive(Debug, Default)]
pub struct ConsumerGroup {
    members: HashMap<MemberId, MemberInfo>,
    next_member_id: MemberId,
    generation: u64,
    partition_count: u32,
}

impl ConsumerGroup {
    pub fn new(partition_count: u32) -> Self {
        Self {
            partition_count,
            ..Default::default()
        }
    }

    pub fn join(&mut self) -> (MemberId, u64, Vec<u32>) {
        self.evict_expired(default_timeout_nanos());
        let id = self.next_member_id;
        self.next_member_id += 1;
        self.members.insert(
            id,
            MemberInfo {
                last_heartbeat_nanos: now_nanos(),
                assignment: Vec::new(),
            },
        );
        self.rebalance();
        let assignment = self.members[&id].assignment.clone();
        (id, self.generation, assignment)
    }

    pub fn heartbeat(&mut self, member_id: MemberId) -> Option<(u64, Vec<u32>)> {
        self.evict_expired(default_timeout_nanos());
        let member = self.members.get_mut(&member_id)?;
        member.last_heartbeat_nanos = now_nanos();
        Some((self.generation, member.assignment.clone()))
    }

    pub fn leave(&mut self, member_id: MemberId) -> bool {
        if self.members.remove(&member_id).is_some() {
            self.rebalance();
            true
        } else {
            false
        }
    }

    pub fn assignment(&self, member_id: MemberId) -> Option<Vec<u32>> {
        self.members.get(&member_id).map(|m| m.assignment.clone())
    }

    #[cfg(test)]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    fn evict_expired(&mut self, timeout_nanos: u64) {
        let now = now_nanos();
        let expired: Vec<MemberId> = self
            .members
            .iter()
            .filter(|(_, m)| now.saturating_sub(m.last_heartbeat_nanos) > timeout_nanos)
            .map(|(id, _)| *id)
            .collect();
        if !expired.is_empty() {
            for id in expired {
                self.members.remove(&id);
            }
            self.rebalance();
        }
    }

    fn rebalance(&mut self) {
        self.generation += 1;
        let mut ids: Vec<MemberId> = self.members.keys().copied().collect();
        ids.sort_unstable();
        // Clear all assignments first
        for m in self.members.values_mut() {
            m.assignment.clear();
        }
        if ids.is_empty() {
            return;
        }
        for p in 0..self.partition_count {
            let owner = ids[(p as usize) % ids.len()];
            self.members.get_mut(&owner).unwrap().assignment.push(p);
        }
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn default_timeout_nanos() -> u64 {
    // 30s — generous for tests; production might bake this into config.
    30 * 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_member_gets_all_partitions() {
        let mut g = ConsumerGroup::new(4);
        let (id, generation, assignment) = g.join();
        assert_eq!(id, 0);
        assert_eq!(generation, 1);
        assert_eq!(assignment, vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_members_split_partitions_round_robin() {
        let mut g = ConsumerGroup::new(4);
        let (a, _, _) = g.join();
        let (b, _, _) = g.join();
        let aa = g.assignment(a).unwrap();
        let ba = g.assignment(b).unwrap();
        assert_eq!(aa, vec![0, 2]);
        assert_eq!(ba, vec![1, 3]);
    }

    #[test]
    fn three_members_distribute_unevenly_for_four_partitions() {
        let mut g = ConsumerGroup::new(4);
        let (_, _, _) = g.join();
        let (_, _, _) = g.join();
        let (_, _, _) = g.join();
        let total: usize = (0..3).map(|i| g.assignment(i as MemberId).unwrap().len()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn leave_triggers_rebalance_and_increments_generation() {
        let mut g = ConsumerGroup::new(4);
        let (a, _, _) = g.join();
        let (b, generation_after_b, _) = g.join();
        assert!(g.leave(a));
        // After leave, b should own everything
        let b_after = g.assignment(b).unwrap();
        assert_eq!(b_after, vec![0, 1, 2, 3]);
        assert!(g.generation() > generation_after_b);
    }

    #[test]
    fn heartbeat_returns_current_assignment() {
        let mut g = ConsumerGroup::new(2);
        let (id, _, _) = g.join();
        let (_, hb_assignment) = g.heartbeat(id).unwrap();
        assert_eq!(hb_assignment, vec![0, 1]);
    }

    #[test]
    fn heartbeat_unknown_member_returns_none() {
        let mut g = ConsumerGroup::new(2);
        assert!(g.heartbeat(99).is_none());
    }
}
