use std::collections::BTreeMap;

use crate::client::ServerError;
use crate::control_plane::consensus::messages::{DeferredReply, DeferredResponse, GetAclSnapshot};
use crate::control_plane::consensus::raft::command::RaftCommand;
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::consensus::raft::state::Raft;
use crate::control_plane::membership::ShardGroupId;

impl GetAclSnapshot {
    fn complete(self, raft: &Raft) -> DeferredReply {
        DeferredReply::GetAclSnapshot(DeferredResponse {
            value: Ok(raft.acl_snapshot(&self.resource)),
            reply: self.reply,
        })
    }

    fn fail(self, error: ServerError) -> DeferredReply {
        DeferredReply::GetAclSnapshot(DeferredResponse {
            value: Err(error),
            reply: self.reply,
        })
    }
}

struct SecurityReadBarrier {
    leader_term: u64,
    open: bool,
    queries: Vec<GetAclSnapshot>,
}

impl SecurityReadBarrier {
    fn new(leader_term: u64, query: GetAclSnapshot) -> Self {
        Self {
            leader_term,
            open: true,
            queries: vec![query],
        }
    }

    fn can_coalesce(&self, leader_term: u64, barrier_index: u64, log_last_index: u64) -> bool {
        self.open && self.leader_term == leader_term && barrier_index == log_last_index
    }

    fn close(&mut self) {
        self.open = false;
    }

    fn push(&mut self, query: GetAclSnapshot) {
        self.queries.push(query);
    }

    fn retain_live_queries(&mut self) -> bool {
        self.queries.retain(|query| !query.reply.is_closed());
        !self.queries.is_empty()
    }

    fn can_resolve(&self, barrier_index: u64, raft: &Raft) -> bool {
        !raft.is_leader()
            || self.leader_term != raft.current_term()
            || barrier_index <= raft.last_applied_index()
    }

    fn resolve(self, raft: &Raft, deferred: &mut Vec<DeferredReply>) {
        if !raft.is_leader() || self.leader_term != raft.current_term() {
            self.fail(ServerError::NotRaftLeader { leader_addr: None }, deferred);
        } else {
            deferred.extend(self.queries.into_iter().map(|query| query.complete(raft)));
        }
    }

    fn fail(self, error: ServerError, deferred: &mut Vec<DeferredReply>) {
        deferred.extend(
            self.queries
                .into_iter()
                .map(|query| query.fail(error.clone())),
        );
    }
}

/// Security queries waiting for their leader-term barrier to be committed and applied.
/// Caller timeouts close the reply channel; every flush prunes those entries.
#[derive(Default)]
pub(super) struct SecurityReadBarriers {
    pending: BTreeMap<(ShardGroupId, u64), SecurityReadBarrier>,
}

impl SecurityReadBarriers {
    /// Queues or coalesces a query. Returns the shard when proposing a barrier
    /// touched Raft and the caller must flush it.
    pub(super) fn queue(
        &mut self,
        query: GetAclSnapshot,
        raft: Option<&mut Raft>,
        deferred: &mut Vec<DeferredReply>,
    ) -> Option<ShardGroupId> {
        let shard_group_id = query.shard_group_id;
        let Some(raft) = raft else {
            deferred.push(query.fail(ProposalError::ShardNotFound.into()));
            return None;
        };
        if !raft.is_leader() {
            deferred.push(query.fail(ProposalError::NotLeader(None).into()));
            return None;
        }

        let leader_term = raft.current_term();
        let log_last_index = raft.log_last_index();
        if let Some((&(_, barrier_index), pending)) = self
            .pending
            .range_mut((shard_group_id, 0)..=(shard_group_id, u64::MAX))
            .next_back()
            && pending.can_coalesce(leader_term, barrier_index, log_last_index)
        {
            pending.push(query);
            return None;
        }

        // The query stays in memory. Only this Noop enters the log, proving the
        // current leader can still commit with a quorum before it serves state.
        match raft.propose(RaftCommand::Noop) {
            Ok(index) => {
                if let Some(replaced) = self.pending.insert(
                    (shard_group_id, index),
                    SecurityReadBarrier::new(leader_term, query),
                ) {
                    replaced.fail(ServerError::NotRaftLeader { leader_addr: None }, deferred);
                }
            }
            Err(error) => deferred.push(query.fail(error.into())),
        }
        Some(shard_group_id)
    }

    pub(super) fn close_all(&mut self) {
        for pending in self.pending.values_mut() {
            pending.close();
        }
    }

    pub(super) fn fail_group(
        &mut self,
        group_id: ShardGroupId,
        error: ServerError,
        deferred: &mut Vec<DeferredReply>,
    ) {
        let keys: Box<[_]> = self
            .pending
            .keys()
            .filter(|(gid, _)| *gid == group_id)
            .copied()
            .collect();
        for key in keys {
            if let Some(pending) = self.pending.remove(&key) {
                pending.fail(error.clone(), deferred);
            }
        }
    }

    pub(super) fn resolve(
        &mut self,
        dirty: &[ShardGroupId],
        groups: &BTreeMap<ShardGroupId, Raft>,
        deferred: &mut Vec<DeferredReply>,
    ) {
        self.pending
            .retain(|_, pending| pending.retain_live_queries());

        for id in dirty {
            let Some(raft) = groups.get(id) else {
                continue;
            };
            let keys: Box<[_]> = self
                .pending
                .range((*id, 0)..)
                .take_while(|((group_id, _), _)| group_id == id)
                .filter_map(|(&(group, barrier_index), read_barrier)| {
                    read_barrier
                        .can_resolve(barrier_index, raft)
                        .then_some((group, barrier_index))
                })
                .collect();

            for key in keys {
                if let Some(pending) = self.pending.remove(&key) {
                    pending.resolve(raft, deferred);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize) {
        (
            self.pending.len(),
            self.pending
                .values()
                .map(|pending| pending.queries.len())
                .sum(),
        )
    }
}
