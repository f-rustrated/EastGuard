# Raft Actor (DS-RSM)

## Purpose

`RaftActor` is async boundary driving multiple `Raft` state machines -- one per shard group on this node. It receives commands from mailbox, feeds into synchronous state machines, flushes all side effects (outbound packets and timer commands).

Actor contains no consensus logic. All decisions live in `Raft`.


**Single actor, multiple state machines:** Instead of spawning one task per shard group, single `RaftActor` multiplexes all groups. Keeps lifecycle management simple, avoids thousands of tokio tasks as shard count grows. Can split later if contention becomes issue.

## Command Types

```rust
pub enum RaftCommand {
    PacketReceived {
        shard_group_id: ShardGroupId,
        from: NodeId,
        rpc: RaftRpc,
    },
    Timeout(RaftTimeoutCallback),
    EnsureGroup { group: ShardGroup },
    RemoveGroup { group_id: ShardGroupId },
    GetLeader {
        group_id: ShardGroupId,
        reply: oneshot::Sender<Option<NodeId>>,
    },
    Propose {
        shard_group_id: ShardGroupId,
        command: messages::RaftCommand,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    HandleNodeDeath { dead_node_id: NodeId },
    HandleNodeJoin {
        new_node_id: NodeId,
        affected_groups: Vec<ShardGroup>,
    },
}
```

## Message Flow

| Variant | Source | Action |
|---|---|---|
| `PacketReceived` | Transport layer (TCP) | Lookup `Raft` by `shard_group_id`, call `raft.step(from, rpc)` |
| `Timeout` | Ticker (timer expiry) | Extract `shard_group_id` from callback, call `raft.handle_timeout(cb)` |
| `EnsureGroup` | SwimActor / HandleNodeJoin | Create `Raft` instance if not exists and this node is member |
| `RemoveGroup` | External | Shut down and remove `Raft` instance |
| `GetLeader` | External callers | Read-only query, returns `Option<NodeId>` via oneshot |
| `Propose` | Client/Coordinator | Lookup `Raft` by `shard_group_id`, call `raft.propose(command)`, reply via oneshot |
| `HandleNodeDeath` | SwimActor (from `MembershipEvent::NodeDead`) | For each group where dead node is peer and this node is leader, propose `RemovePeer(dead_node_id)` |
| `HandleNodeJoin` | SwimActor (from `MembershipEvent::NodeAlive`) | For existing groups where this node is leader and new node not yet a peer: propose `AddPeer`. For groups this node should newly join: `EnsureGroup`. |


## Transport: TCP

Raft RPCs go over TCP (reliable, ordered delivery). `RaftTransportActor` listens on dedicated `raft_port`. Each packet tagged with `ShardGroupId` so transport layer routes to correct group.

Separate from SWIM's UDP transport (`SwimTransportActor` on `cluster_port`).

## Leader Change Detection

Actor monitors `Raft::role()` after each event. When group's role transitions to/from `Leader`, actor emits `ShardLeaderEvent` for SWIM gossip:

```rust
let was_leader = old_role == Role::Leader;
let is_leader = raft.is_leader();
if was_leader != is_leader {
    // Emit ShardLeaderEvent to SWIM gossip layer
}
```

Phase 6 will connect Raft elections to SWIM's `ShardLeader` piggybacking (#35-#38).

## Relationship to Other Actors

```
SwimActor (UDP, membership) ──raft_tx──> RaftActor (TCP, consensus)
       |    (HandleNodeDeath,                  |
       |     HandleNodeJoin)                   |
       v                                       v
  SwimTransportActor                    RaftTransportActor
  (cluster_port, UDP)                   (raft_port, TCP)
       |                                       |
       +------------ Ticker <-----------------+
                (shared scheduler)
```

Both actors share same `Ticker`-based scheduler (via `run_scheduling_actor`), but use different timer types (`SwimTimer` vs `RaftTimer`). Timer seq namespacing prevents collisions.

SwimActor sends membership-driven commands directly to RaftActor — no intermediate bridge. Topology queries are local (`state.topology.shard_groups_for_node()`).

## Peer Resolution: NodeId → Connection

`Raft` state machine is fully transport-agnostic. Identifies peers by `NodeId` only — no `SocketAddr`(or like) in state machine. Resolution chain:

```
SWIM membership table          (NodeId → SocketAddr, maintained by gossip)
       |
       v
Topology / hash ring           (key → ShardGroup { members: Vec<NodeId> })
       |
       v
SwimActor                      (sends HandleNodeDeath/HandleNodeJoin to RaftActor)
       |
       v
RaftActor                      (maintains NodeId → Connection pool)
       |                        resolves OutboundRaftPacket.target (NodeId)
       v                        to a persistent TCP connection
RaftTransportActor              (TCP I/O)
```

**Key points:**

- `Raft::peers` is `HashSet<NodeId>` — no addresses. State machine produces `OutboundRaftPacket { target: NodeId, rpc }`. Actor/transport layer resolves `NodeId → SocketAddr → Connection`.

- Connection pool (`HashMap<NodeId, RaftWriter>`) lives in `RaftTransportActor`. Connections persistent and bidirectional — one TCP connection per peer pair, split into `RaftReader`/`RaftWriter` halves.

- **Handshake**: On connect, initiator sends its `NodeId` (length-prefixed bincode). Acceptor reads it to key write half. Happens before any Raft RPCs.

- **Conflict resolution**: Either side can initiate connection. On simultaneous connect, connection initiated by **lower `NodeId`** wins. Acceptor checks: if writer for peer already exists and `peer_id > self.node_id`, incoming connection dropped (we initiated as lower NodeId, ours takes precedence).

- SWIM is authoritative source of `NodeId → SocketAddr` (via `SwimQueryCommand::ResolveAddress`). When node's address changes, SWIM detects via gossip.

- **ConfChange (AddPeer/RemovePeer)**: `RemovePeer(NodeId)` proposed by leader on `HandleNodeDeath`, `AddPeer(NodeId)` proposed on `HandleNodeJoin`. Both applied on commit via `apply_committed_entries()` — modifies `Raft::peers` and `peer_states` in-place. Group survives with modified membership, no `RemoveGroup` + `EnsureGroup` needed.

## Invariants

1. **Actor is sole driver of all Raft instances.** No other code calls `step()`, `handle_timeout()`, or `propose()` on `Raft` instance in production.

2. **Every event must be followed by flush.** `Raft` state machine buffers all side effects internally. Skip flush → packets and timer commands silently lost.

3. **Raft instances are independent.** Processing event for `Shard #12` never touches `Shard #45`'s state.

4. **Group lifecycle controlled by SwimActor.** RaftActor does not decide when to create or remove groups. Only responds to `EnsureGroup`, `RemoveGroup`, `HandleNodeDeath`, and `HandleNodeJoin` commands sent by SwimActor.

5. **Timer commands flow through scheduler.** Actor converts `TimerCommand<RaftTimer>` into `TickerCommand<RaftTimer>` (via `.into()`) and sends to shared ticker. Ticker fires `RaftTimeoutCallback` back into actor's mailbox.