#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::state::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId, ShardToken};
use crate::schedulers::ticker_message::{TickerCommand, TimerCommand};

use tokio::sync::{mpsc, oneshot};

/// Commands received by the RaftActor from external sources.
pub enum RaftCommand {
    /// An RPC arrived from a peer via the transport layer.
    PacketReceived {
        shard_group_id: ShardGroupId,
        from: NodeId,
        rpc: RaftRpc,
    },
    /// A timer expired (election or heartbeat).
    Timeout(RaftTimeoutCallback),
    /// Create a Raft group if this node is a member.
    EnsureGroup { group: ShardGroup },
    /// Remove a Raft group.
    RemoveGroup { group_id: ShardGroupId },
    /// Query the current leader of a shard group.
    GetLeader {
        group_id: ShardGroupId,
        reply: oneshot::Sender<Option<NodeId>>,
    },
    /// Propose a command to a shard group's Raft log. Leader-only.
    Propose {
        shard_group_id: ShardGroupId,
        command: crate::clusters::raft::messages::RaftCommand,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    /// A node died — remove it from all groups where this node is leader.
    HandleNodeDeath { dead_node_id: NodeId },
    /// A node joined — add it to groups where this node is leader and the
    /// new node should be a member. Also EnsureGroup for groups this node
    /// should newly participate in.
    HandleNodeJoin {
        new_node_id: NodeId,
        affected_groups: Vec<ShardGroup>,
    },
}

impl From<RaftTimeoutCallback> for RaftCommand {
    fn from(cb: RaftTimeoutCallback) -> Self {
        RaftCommand::Timeout(cb)
    }
}

/// Async boundary that multiplexes multiple Raft state machines — one per
/// shard group. Follows the same pattern as `SwimActor`.
///
/// Timer seqs are namespaced: each `Raft` emits local seqs (0 = election,
/// 1 = heartbeat). The actor translates these to globally unique seqs via a
/// monotonic counter, preventing collisions across groups in the shared Ticker.
pub struct RaftActor {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, Raft<crate::storage::MemoryLogStore>>,
    mailbox: mpsc::Receiver<RaftCommand>,
    transport_tx: mpsc::Sender<OutboundRaftPacket>,
    scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
    // Timer seq namespacing: (ShardGroupId, local_seq) → global_seq
    // Every Raft instance emits the same local seqs — 0 for election timer, 1 for heartbeat timer.
    // If shard group #12 and shard group #45 both emit SetSchedule { seq: 0 }, the Ticker (which stores timers in HashMap<u32,RaftTimer>) would overwrite one with the other.
    //
    // So,
    //  Shard #12 emits: SetSchedule { seq: 0 (election) }
    //     → Actor translates: seq 0 → global_seq 1
    //     → Stores mapping: (ShardGroupId(12), 0) → 1
    //     → Sends to Ticker: SetSchedule { seq: 1 }

    //   Shard #45 emits: SetSchedule { seq: 0 (election) }
    //     → Actor translates: seq 0 → global_seq 2
    //     → Stores mapping: (ShardGroupId(45), 0) → 2
    //     → Sends to Ticker: SetSchedule { seq: 2 }
    seq_counter: u32,
    shard_tokens: HashMap<ShardToken, u32>,
}

impl RaftActor {
    pub fn new(
        node_id: NodeId,
        mailbox: mpsc::Receiver<RaftCommand>,
        transport_tx: mpsc::Sender<OutboundRaftPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
    ) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            mailbox,
            transport_tx,
            scheduler_tx,
            seq_counter: 0,
            shard_tokens: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.mailbox.recv().await {
            match cmd {
                RaftCommand::PacketReceived {
                    shard_group_id,
                    from,
                    rpc,
                } => {
                    if let Some(raft) = self.groups.get_mut(&shard_group_id) {
                        raft.step(from, rpc);
                        self.flush(shard_group_id).await;
                    }
                }

                RaftCommand::Timeout(cb) => {
                    let shard_group_id = match &cb {
                        RaftTimeoutCallback::Ignored => continue,
                        RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
                        RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
                    };
                    if let Some(raft) = self.groups.get_mut(&shard_group_id) {
                        raft.handle_timeout(cb);
                        self.flush(shard_group_id).await;
                    }
                }

                RaftCommand::EnsureGroup { group } => {
                    let group_id = group.id;
                    self.ensure_group(group);
                    self.flush(group_id).await;
                }

                RaftCommand::RemoveGroup { group_id } => {
                    self.remove_group(group_id).await;
                }

                RaftCommand::GetLeader { group_id, reply } => {
                    let leader = self
                        .groups
                        .get(&group_id)
                        .and_then(|raft| raft.current_leader().cloned());
                    let _ = reply.send(leader);
                }

                RaftCommand::Propose {
                    shard_group_id,
                    command,
                    reply,
                } => {
                    let result = match self.groups.get_mut(&shard_group_id) {
                        Some(raft) => raft.propose(command),
                        None => Err(ProposeError::NotLeader),
                    };
                    let _ = reply.send(result);
                    self.flush(shard_group_id).await;
                }

                RaftCommand::HandleNodeDeath { dead_node_id } => {
                    // Collect affected group IDs first (borrow checker).
                    let affected: Vec<ShardGroupId> = self
                        .groups
                        .iter()
                        .filter(|(_, raft)| raft.is_leader() && raft.has_peer(&dead_node_id))
                        .map(|(id, _)| *id)
                        .collect();

                    for group_id in &affected {
                        if let Some(raft) = self.groups.get_mut(group_id) {
                            let cmd = crate::clusters::raft::messages::RaftCommand::RemovePeer(
                                dead_node_id.clone(),
                            );
                            let _ = raft.propose(cmd);
                        }
                    }

                    for group_id in affected {
                        self.flush(group_id).await;
                    }
                }

                RaftCommand::HandleNodeJoin {
                    new_node_id,
                    affected_groups,
                } => {
                    let mut flushed = Vec::new();

                    for group in &affected_groups {
                        if let Some(raft) = self.groups.get_mut(&group.id)
                            && raft.is_leader()
                            && !raft.has_peer(&new_node_id)
                        {
                            let cmd = crate::clusters::raft::messages::RaftCommand::AddPeer(
                                new_node_id.clone(),
                            );
                            let _ = raft.propose(cmd);
                            flushed.push(group.id);
                        }
                    }

                    // EnsureGroup for groups this node should newly join.
                    for group in affected_groups {
                        if !self.groups.contains_key(&group.id)
                            && group.members.contains(&self.node_id)
                        {
                            let group_id = group.id;
                            self.ensure_group(group);
                            flushed.push(group_id);
                        }
                    }

                    for group_id in flushed {
                        self.flush(group_id).await;
                    }
                }
            }
        }
    }

    fn ensure_group(&mut self, group: ShardGroup) {
        if self.groups.contains_key(&group.id) {
            return;
        }
        if !group.members.contains(&self.node_id) {
            return;
        }

        let peers: HashSet<NodeId> = group
            .members
            .iter()
            .filter(|id| *id != &self.node_id)
            .cloned()
            .collect();

        // Derive jitter from node_id hash to spread election timeouts.
        // A proper hash avoids collisions that would cause split votes (e.g.
        // same-length names, sequential names like "node-1"/"node-2").
        let jitter = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            (hasher.finish() % 20) as u32
        };
        let raft = Raft::new(self.node_id.clone(), peers, jitter, crate::storage::MemoryLogStore::default(), group.id);

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            self.node_id,
            group.id,
            raft.peers_count()
        );

        self.groups.insert(group.id, raft);
    }

    async fn remove_group(&mut self, group_id: ShardGroupId) {
        if self.groups.remove(&group_id).is_none() {
            return;
        }

        // Cancel any outstanding timers for this group
        for local_seq in [0, 1] {
            if let Some(global_seq) = self.shard_tokens.remove(&group_id.token(local_seq)) {
                let _ = self
                    .scheduler_tx
                    .send(TimerCommand::CancelSchedule { seq: global_seq }.into())
                    .await;
            }
        }

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    /// Flush outbound packets and timer commands for one shard group.
    async fn flush(&mut self, shard_group_id: ShardGroupId) {
        let raft = match self.groups.get_mut(&shard_group_id) {
            Some(r) => r,
            None => return,
        };

        let timer_commands = raft.take_timer_commands();
        let outbound_packets = raft.take_outbound();

        // Translate local seqs → global seqs (needs &mut self, so done before the concurrent send)
        let translated: Vec<_> = timer_commands
            .into_iter()
            .filter_map(|cmd| match cmd {
                TimerCommand::SetSchedule {
                    seq: local_seq,
                    timer,
                } => Some(TimerCommand::SetSchedule {
                    seq: self.get_or_alloc_seq(shard_group_id.token(local_seq)),
                    timer,
                }),
                TimerCommand::CancelSchedule { seq: local_seq } => self
                    .shard_tokens
                    .get(&shard_group_id.token(local_seq))
                    .map(|&global_seq| TimerCommand::CancelSchedule { seq: global_seq }),
            })
            .collect();

        // Send timer commands and outbound packets concurrently
        tokio::join!(
            async {
                for cmd in translated {
                    let _ = self.scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    let _ = self.transport_tx.send(pkt).await;
                }
            }
        );
    }

    /// Returns a stable global seq for the given `(shard_group_id, local_seq)`.
    /// Allocates one on first call, reuses it on subsequent calls.
    fn get_or_alloc_seq(&mut self, token: ShardToken) -> u32 {
        *self.shard_tokens.entry(token).or_insert_with(|| {
            self.seq_counter = self.seq_counter.wrapping_add(1);
            self.seq_counter
        })
    }
}
