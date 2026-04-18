#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::state::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId, ShardToken, SwimCommand};
use crate::schedulers::ticker_message::{TickerCommand, TimerCommand};

use tokio::sync::{mpsc, oneshot};

/// Commands received by the MultiRaftActor from external sources.
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

/// Multiplexes multiple Raft state machines — one per shard group.
///
/// Manages group lifecycle, timer seq namespacing, and dirty-group tracking
/// for drain-then-flush batching.
///
/// Timer seqs are namespaced: each `Raft` emits local seqs (0 = election,
/// 1 = heartbeat). This struct translates them to globally unique seqs via a
/// monotonic counter, preventing collisions across groups in the shared Ticker.
struct RaftGroups {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, Raft>,
    /// Monotonic counter for globally unique timer seqs.
    seq_counter: u32,
    /// Maps (ShardGroupId, local_seq) → global_seq for timer namespacing.
    shard_tokens: HashMap<ShardToken, u32>,

    // Groups modified since last flush.
    dirty: HashSet<ShardGroupId>,
    /// Outbound packets aggregated by target NodeId for batched transport sends.
    packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
}

impl RaftGroups {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            seq_counter: 0,
            shard_tokens: HashMap::new(),
            dirty: HashSet::new(),
            packets_by_target: Default::default(),
        }
    }

    async fn process_command(
        &mut self,
        cmd: RaftCommand,
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
    ) {
        match cmd {
            RaftCommand::PacketReceived {
                shard_group_id,
                from,
                rpc,
            } => {
                if let Some(raft) = self.groups.get_mut(&shard_group_id) {
                    raft.step(from, rpc);
                    self.dirty.insert(shard_group_id);
                }
            }

            RaftCommand::Timeout(cb) => {
                let shard_group_id = match &cb {
                    RaftTimeoutCallback::Ignored => return,
                    RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
                    RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
                };
                if let Some(raft) = self.groups.get_mut(&shard_group_id) {
                    raft.handle_timeout(cb);
                    self.dirty.insert(shard_group_id);
                }
            }

            RaftCommand::EnsureGroup { group } => {
                let group_id = group.id;
                self.ensure_group(group);
                self.dirty.insert(group_id);
            }

            RaftCommand::RemoveGroup { group_id } => {
                self.remove_group(group_id, scheduler_tx).await;
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
                self.dirty.insert(shard_group_id);
            }

            RaftCommand::HandleNodeDeath { dead_node_id } => {
                // Evict stale connection + cached address so the transport
                // stops sending RPCs to the dead node's address.
                let _ = transport_tx
                    .send(RaftTransportCommand::DisconnectPeer(dead_node_id.clone()))
                    .await;

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

                self.dirty.extend(affected);
            }

            RaftCommand::HandleNodeJoin {
                new_node_id,
                affected_groups,
            } => {
                for group in &affected_groups {
                    if let Some(raft) = self.groups.get_mut(&group.id)
                        && raft.is_leader()
                        && !raft.has_peer(&new_node_id)
                    {
                        let cmd = crate::clusters::raft::messages::RaftCommand::AddPeer(
                            new_node_id.clone(),
                        );
                        let _ = raft.propose(cmd);
                        self.dirty.insert(group.id);
                    }
                }

                for group in affected_groups {
                    if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id)
                    {
                        let group_id = group.id;
                        self.ensure_group(group);
                        self.dirty.insert(group_id);
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

        let jitter = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            (hasher.finish() % 20) as u32
        };
        let raft = Raft::new(self.node_id.clone(), peers, jitter, group.id);

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            self.node_id,
            group.id,
            raft.peers_count()
        );

        self.groups.insert(group.id, raft);
    }

    async fn remove_group(
        &mut self,
        group_id: ShardGroupId,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
    ) {
        if self.groups.remove(&group_id).is_none() {
            return;
        }

        for local_seq in [0, 1] {
            if let Some(global_seq) = self.shard_tokens.remove(&group_id.token(local_seq)) {
                let _ = scheduler_tx
                    .send(TimerCommand::CancelSchedule { seq: global_seq }.into())
                    .await;
            }
        }

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    /// Flush all side-effects for dirty groups.
    ///
    /// Drains a single `RaftEvent` stream per group and routes each variant
    /// to the appropriate channel. Outbound packets are aggregated by target
    /// NodeId — 200 shard groups producing messages for the same 2 physical
    /// nodes result in 2 batched channel sends, not 200 individual ones.
    async fn flush_dirty(
        &mut self,
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: &mpsc::Sender<SwimCommand>,
    ) {
        let to_flush = std::mem::take(&mut self.dirty);

        for group_id in to_flush {
            let Some(raft) = self.groups.get_mut(&group_id) else {
                continue;
            };
            for event in raft.take_events() {
                match event {
                    RaftEvent::OutboundRaftPacket(pkt) => {
                        self.packets_by_target
                            .entry(pkt.target.clone())
                            .or_default()
                            .push(pkt);
                    }
                    RaftEvent::Timer(cmd) => {
                        if let Some(cmd) = self.translate_timer_seq(group_id, cmd) {
                            let _ = scheduler_tx.send(cmd.into()).await;
                        }
                    }
                    RaftEvent::LeaderChange(lc) => {
                        let _ = swim_tx.send(SwimCommand::AnnounceShardLeader(lc)).await;
                    }
                }
            }
        }

        for (_, packets) in self.packets_by_target.drain() {
            let _ = transport_tx.send(RaftTransportCommand::Send(packets)).await;
        }
    }

    /// Translate local timer seqs to globally unique seqs for the shared Ticker.
    fn translate_timer_seq(
        &mut self,
        group_id: ShardGroupId,
        cmd: TimerCommand<RaftTimer>,
    ) -> Option<TimerCommand<RaftTimer>> {
        match cmd {
            TimerCommand::SetSchedule {
                seq: local_seq,
                timer,
            } => Some(TimerCommand::SetSchedule {
                seq: self.get_or_alloc_seq(group_id.token(local_seq)),
                timer,
            }),
            TimerCommand::CancelSchedule { seq: local_seq } => self
                .shard_tokens
                .get(&group_id.token(local_seq))
                .map(|&global_seq| TimerCommand::CancelSchedule { seq: global_seq }),
        }
    }

    fn get_or_alloc_seq(&mut self, token: ShardToken) -> u32 {
        *self.shard_tokens.entry(token).or_insert_with(|| {
            self.seq_counter = self.seq_counter.wrapping_add(1);
            self.seq_counter
        })
    }
}

/// Async boundary — receives commands from mailbox, delegates to `RaftGroups`.
pub struct MultiRaftActor;

impl MultiRaftActor {
    pub async fn run(
        node_id: NodeId,
        mut mailbox: mpsc::Receiver<RaftCommand>,
        transport_tx: mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: mpsc::Sender<SwimCommand>,
    ) {
        let mut state = RaftGroups::new(node_id);
        let mut buf = Vec::with_capacity(64);

        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for cmd in buf.drain(..) {
                state
                    .process_command(cmd, &transport_tx, &scheduler_tx)
                    .await;
            }
            state
                .flush_dirty(&transport_tx, &scheduler_tx, &swim_tx)
                .await;
        }
    }
}
