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
/// shard group.
///
/// Timer seqs are namespaced: each `Raft` emits local seqs (0 = election,
/// 1 = heartbeat). The actor translates these to globally unique seqs via a
/// monotonic counter, preventing collisions across groups in the shared Ticker.
pub struct RaftActor;

impl RaftActor {
    pub async fn run(
        node_id: NodeId,
        mut mailbox: mpsc::Receiver<RaftCommand>,
        transport_tx: mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
    ) {
        let mut groups: HashMap<ShardGroupId, Raft<crate::storage::MemoryLogStore>> =
            HashMap::new();

        // Timer seq namespacing: (ShardGroupId, local_seq) → global_seq
        // Every Raft instance emits the same local seqs — 0 for election timer,
        // 1 for heartbeat timer. The actor translates to globally unique seqs so
        // the shared Ticker doesn't overwrite one group's timer with another's.
        let mut seq_counter: u32 = 0;
        let mut shard_tokens: HashMap<ShardToken, u32> = HashMap::new();

        while let Some(cmd) = mailbox.recv().await {
            match cmd {
                RaftCommand::PacketReceived {
                    shard_group_id,
                    from,
                    rpc,
                } => {
                    if let Some(raft) = groups.get_mut(&shard_group_id) {
                        raft.step(from, rpc);
                        Self::flush(
                            shard_group_id,
                            &mut groups,
                            &mut seq_counter,
                            &mut shard_tokens,
                            &transport_tx,
                            &scheduler_tx,
                        )
                        .await;
                    }
                }

                RaftCommand::Timeout(cb) => {
                    let shard_group_id = match &cb {
                        RaftTimeoutCallback::Ignored => continue,
                        RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
                        RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
                    };
                    if let Some(raft) = groups.get_mut(&shard_group_id) {
                        raft.handle_timeout(cb);
                        Self::flush(
                            shard_group_id,
                            &mut groups,
                            &mut seq_counter,
                            &mut shard_tokens,
                            &transport_tx,
                            &scheduler_tx,
                        )
                        .await;
                    }
                }

                RaftCommand::EnsureGroup { group } => {
                    let group_id = group.id;
                    Self::ensure_group(&node_id, &mut groups, group);
                    Self::flush(
                        group_id,
                        &mut groups,
                        &mut seq_counter,
                        &mut shard_tokens,
                        &transport_tx,
                        &scheduler_tx,
                    )
                    .await;
                }

                RaftCommand::RemoveGroup { group_id } => {
                    Self::remove_group(
                        &node_id,
                        group_id,
                        &mut groups,
                        &mut shard_tokens,
                        &scheduler_tx,
                    )
                    .await;
                }

                RaftCommand::GetLeader { group_id, reply } => {
                    let leader = groups
                        .get(&group_id)
                        .and_then(|raft| raft.current_leader().cloned());
                    let _ = reply.send(leader);
                }

                RaftCommand::Propose {
                    shard_group_id,
                    command,
                    reply,
                } => {
                    let result = match groups.get_mut(&shard_group_id) {
                        Some(raft) => raft.propose(command),
                        None => Err(ProposeError::NotLeader),
                    };
                    let _ = reply.send(result);
                    Self::flush(
                        shard_group_id,
                        &mut groups,
                        &mut seq_counter,
                        &mut shard_tokens,
                        &transport_tx,
                        &scheduler_tx,
                    )
                    .await;
                }

                RaftCommand::HandleNodeDeath { dead_node_id } => {
                    // Evict stale connection + cached address so the transport
                    // stops sending RPCs to the dead node's address.
                    let _ = transport_tx
                        .send(RaftTransportCommand::DisconnectPeer(dead_node_id.clone()))
                        .await;

                    let affected: Vec<ShardGroupId> = groups
                        .iter()
                        .filter(|(_, raft)| raft.is_leader() && raft.has_peer(&dead_node_id))
                        .map(|(id, _)| *id)
                        .collect();

                    for group_id in &affected {
                        if let Some(raft) = groups.get_mut(group_id) {
                            let cmd = crate::clusters::raft::messages::RaftCommand::RemovePeer(
                                dead_node_id.clone(),
                            );
                            let _ = raft.propose(cmd);
                        }
                    }

                    for group_id in affected {
                        Self::flush(
                            group_id,
                            &mut groups,
                            &mut seq_counter,
                            &mut shard_tokens,
                            &transport_tx,
                            &scheduler_tx,
                        )
                        .await;
                    }
                }

                RaftCommand::HandleNodeJoin {
                    new_node_id,
                    affected_groups,
                } => {
                    let mut flushed = Vec::new();

                    for group in &affected_groups {
                        if let Some(raft) = groups.get_mut(&group.id)
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

                    for group in affected_groups {
                        if !groups.contains_key(&group.id)
                            && group.members.contains(&node_id)
                        {
                            let group_id = group.id;
                            Self::ensure_group(&node_id, &mut groups, group);
                            flushed.push(group_id);
                        }
                    }

                    for group_id in flushed {
                        Self::flush(
                            group_id,
                            &mut groups,
                            &mut seq_counter,
                            &mut shard_tokens,
                            &transport_tx,
                            &scheduler_tx,
                        )
                        .await;
                    }
                }
            }
        }
    }

    fn ensure_group(
        node_id: &NodeId,
        groups: &mut HashMap<ShardGroupId, Raft<crate::storage::MemoryLogStore>>,
        group: ShardGroup,
    ) {
        if groups.contains_key(&group.id) {
            return;
        }
        if !group.members.contains(node_id) {
            return;
        }

        let peers: HashSet<NodeId> = group
            .members
            .iter()
            .filter(|id| *id != node_id)
            .cloned()
            .collect();

        let jitter = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            node_id.hash(&mut hasher);
            (hasher.finish() % 20) as u32
        };
        let raft = Raft::new(
            node_id.clone(),
            peers,
            jitter,
            crate::storage::MemoryLogStore::default(),
            group.id,
        );

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            node_id,
            group.id,
            raft.peers_count()
        );

        groups.insert(group.id, raft);
    }

    async fn remove_group(
        node_id: &NodeId,
        group_id: ShardGroupId,
        groups: &mut HashMap<ShardGroupId, Raft<crate::storage::MemoryLogStore>>,
        shard_tokens: &mut HashMap<ShardToken, u32>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
    ) {
        if groups.remove(&group_id).is_none() {
            return;
        }

        for local_seq in [0, 1] {
            if let Some(global_seq) = shard_tokens.remove(&group_id.token(local_seq)) {
                let _ = scheduler_tx
                    .send(TimerCommand::CancelSchedule { seq: global_seq }.into())
                    .await;
            }
        }

        tracing::info!("[{}] Removed Raft group {:?}", node_id, group_id);
    }

    /// Flush outbound packets and timer commands for one shard group.
    async fn flush(
        shard_group_id: ShardGroupId,
        groups: &mut HashMap<ShardGroupId, Raft<crate::storage::MemoryLogStore>>,
        seq_counter: &mut u32,
        shard_tokens: &mut HashMap<ShardToken, u32>,
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
    ) {
        let raft = match groups.get_mut(&shard_group_id) {
            Some(r) => r,
            None => return,
        };

        let timer_commands = raft.take_timer_commands();
        let outbound_packets = raft.take_outbound();

        let translated: Vec<_> = timer_commands
            .into_iter()
            .filter_map(|cmd| match cmd {
                TimerCommand::SetSchedule {
                    seq: local_seq,
                    timer,
                } => Some(TimerCommand::SetSchedule {
                    seq: Self::get_or_alloc_seq(
                        shard_group_id.token(local_seq),
                        seq_counter,
                        shard_tokens,
                    ),
                    timer,
                }),
                TimerCommand::CancelSchedule { seq: local_seq } => shard_tokens
                    .get(&shard_group_id.token(local_seq))
                    .map(|&global_seq| TimerCommand::CancelSchedule { seq: global_seq }),
            })
            .collect();

        tokio::join!(
            async {
                for cmd in translated {
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    let _ = transport_tx.send(RaftTransportCommand::Send(pkt)).await;
                }
            }
        );
    }

    fn get_or_alloc_seq(
        token: ShardToken,
        seq_counter: &mut u32,
        shard_tokens: &mut HashMap<ShardToken, u32>,
    ) -> u32 {
        *shard_tokens.entry(token).or_insert_with(|| {
            *seq_counter = seq_counter.wrapping_add(1);
            *seq_counter
        })
    }
}
