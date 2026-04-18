#![allow(dead_code)]

use std::collections::HashMap;

use crate::clusters::NodeId;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::multi_raft::MultiRaft;
use crate::clusters::swims::{ShardGroup, ShardGroupId, SwimCommand};
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::{mpsc, oneshot};

/// Commands received by the MultiRaftActor from external sources.
pub enum MultiRaftActorCommand {
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

impl From<RaftTimeoutCallback> for MultiRaftActorCommand {
    fn from(cb: RaftTimeoutCallback) -> Self {
        MultiRaftActorCommand::Timeout(cb)
    }
}

/// Async boundary — receives commands from mailbox, delegates to `MultiRaftStore`.
pub struct MultiRaftActor;

impl MultiRaftActor {
    pub async fn run(
        node_id: NodeId,
        mut mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        transport_tx: mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: mpsc::Sender<SwimCommand>,
    ) {
        let mut store = MultiRaft::new(node_id);
        let mut buf = Vec::with_capacity(64);

        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }

            for cmd in buf.drain(..) {
                match cmd {
                    MultiRaftActorCommand::PacketReceived {
                        shard_group_id,
                        from,
                        rpc,
                    } => {
                        store.step(shard_group_id, from, rpc);
                    }
                    MultiRaftActorCommand::Timeout(cb) => {
                        store.handle_timeout(cb);
                    }
                    MultiRaftActorCommand::EnsureGroup { group } => {
                        store.add_group(group);
                    }
                    MultiRaftActorCommand::RemoveGroup { group_id } => {
                        store.remove_group(group_id);
                    }
                    MultiRaftActorCommand::GetLeader { group_id, reply } => {
                        let _ = reply.send(store.get_leader(group_id));
                    }
                    MultiRaftActorCommand::Propose {
                        shard_group_id,
                        command,
                        reply,
                    } => {
                        let _ = reply.send(store.propose(shard_group_id, command));
                    }
                    MultiRaftActorCommand::HandleNodeDeath { dead_node_id } => {
                        // Evict stale connection — transport concern, not store concern.
                        let _ = transport_tx
                            .send(RaftTransportCommand::DisconnectPeer(dead_node_id.clone()))
                            .await;
                        store.remove_node(dead_node_id);
                    }
                    MultiRaftActorCommand::HandleNodeJoin {
                        new_node_id,
                        affected_groups,
                    } => {
                        store.add_node(new_node_id, affected_groups);
                    }
                }
            }

            let mut packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>> = HashMap::new();

            for event in store.flush() {
                match event {
                    RaftEvent::OutboundRaftPacket(pkt) => {
                        packets_by_target
                            .entry(pkt.target.clone())
                            .or_default()
                            .push(pkt);
                    }
                    RaftEvent::Timer(cmd) => {
                        let _ = scheduler_tx.send(cmd.into()).await;
                    }
                    RaftEvent::LeaderChange(lc) => {
                        let _ = swim_tx.send(SwimCommand::AnnounceShardLeader(lc)).await;
                    }
                }
            }

            for (_, packets) in packets_by_target {
                let _ = transport_tx.send(RaftTransportCommand::Send(packets)).await;
            }
        }
    }
}
