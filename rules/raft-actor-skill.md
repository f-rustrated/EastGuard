# Raft Actor (DS-RSM)

## Purpose

`RaftActor` is async boundary driving multiple `Raft` state machines -- one per shard group on this node. Same pattern as `SwimActor`: receives commands from mailbox, feeds into synchronous state machines, flushes all side effects (outbound packets and timer commands).

Actor contains no consensus logic. All decisions live in `Raft`.

## Architecture

```
                       mpsc::Receiver<RaftCommand>
                                 |
                                 v
                          +-----------+
                          | RaftActor |
                          +-----------+
                          |  mailbox  |
                          +-----------+
                                 |
                     dispatches by ShardGroupId
                                 |
                    +------------+------------+
                    |            |            |
               Raft #12    Raft #45    Raft #78
               (Leader)   (Follower)  (Leader)
                    |            |            |
                    +-----+------+-----+------+
                          |            |
              take_timer_commands()  take_outbound()
                          |            |
                          v            v
        mpsc::Sender<TickerCommand>  mpsc::Sender<OutboundRaftPacket>
           (scheduler_tx)               (transport_tx)
```

**Single actor, multiple state machines:** Instead of spawning one task per shard group, single `RaftActor` multiplexes all groups. Keeps lifecycle management simple, avoids thousands of tokio tasks as shard count grows. Can split later if contention becomes issue.

## Command Types

```rust
pub enum RaftCommand {
    PacketReceived {
        shard_id: ShardGroupId,
        src: SocketAddr,
        rpc: RaftRpc,
    },
    Timeout {
        shard_id: ShardGroupId,
        callback: RaftTimeoutCallback,
    },
    Propose {
        shard_id: ShardGroupId,
        command: RaftCommand,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    Query(RaftQueryCommand),
    EnsureGroup {
        group: ShardGroup,
    },
    RemoveGroup {
        group_id: ShardGroupId,
    },
}
```

## Message Flow

| Variant | Source | Action |
|---|---|---|
| `PacketReceived` | Transport layer (TCP) | Lookup `Raft` by `shard_id`, call `raft.step(src, rpc)` |
| `Timeout` | Ticker (timer expiry) | Lookup `Raft` by `shard_id`, call `raft.handle_timeout(callback)` |
| `Propose` | Client/Coordinator | Lookup `Raft` by `shard_id`, call `raft.propose(command)`, reply via oneshot |
| `Query` | External callers | Read-only queries (e.g., `GetLeader { shard_id }`) |
| `EnsureGroup` | Raft-Topology Bridge | Create `Raft` instance if not exists and this node is member |
| `RemoveGroup` | Raft-Topology Bridge | Shut down and remove `Raft` instance |

## Flush Protocol

After every command, actor must flush **all** groups that were touched:

```rust
fn flush(&mut self, shard_id: ShardGroupId) {
    if let Some(raft) = self.groups.get_mut(&shard_id) {
        for pkt in raft.take_outbound() {
            let _ = self.transport_tx.send(pkt).await;
        }
        for cmd in raft.take_timer_commands() {
            // Timer commands must be scoped to shard_id to avoid collision
            // across groups (e.g., prefix seq with shard_id hash)
            let _ = self.scheduler_tx.send(cmd.into()).await;
        }
    }
}
```

## Timer Seq Namespacing

Each `Raft` instance uses fixed seq values (`ELECTION_TIMER_SEQ = 0`, `HEARTBEAT_TIMER_SEQ = 1`). Multiple groups share same scheduler, so actor must **namespace** these seqs to avoid collisions:

```
effective_seq = (shard_group_id_hash << 2) | local_seq
```

Ensures `Shard #12`'s election timer doesn't cancel `Shard #45`'s election timer.

## Transport: TCP, not UDP

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

Bridge connecting Raft elections to SWIM's `ShardLeader` piggybacking (#35-#38).

## Relationship to Other Actors

```
SwimActor (UDP, membership)     RaftActor (TCP, consensus)
       |                               |
       |                               |
       v                               v
  SwimTransportActor            RaftTransportActor
  (cluster_port, UDP)           (raft_port, TCP)
       |                               |
       +---------- Ticker <------------+
              (shared scheduler)
```

Both actors share same `Ticker`-based scheduler (via `run_scheduling_actor`), but use different timer types (`SwimTimer` vs `RaftTimer`). Timer seq namespacing prevents collisions.

## Peer Resolution: NodeId → Connection

`Raft` state machine is fully transport-agnostic. Identifies peers by `NodeId` only — no `SocketAddr`(or like) in state machine. Resolution chain:

```
SWIM membership table          (NodeId → SocketAddr, maintained by gossip)
       |
       v
Topology / hash ring           (key → ShardGroup { members: Vec<NodeId> })
       |
       v
Raft-Topology Bridge           (looks up each member's SocketAddr from SWIM)
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

- SWIM is authoritative source of `NodeId → SocketAddr` (via `SwimQueryCommand::ResolveAddress`). When node's address changes, SWIM detects via gossip. Bridge recreates affected shard groups.

- For ConfChanges (adding/removing peer mid-term without destroying group), `Raft` will need `add_peer(NodeId)` / `remove_peer(NodeId)` methods. Until then, membership changes handled by destroying and recreating `Raft` instance via `RemoveGroup` + `EnsureGroup`.

## Invariants

1. **Actor is sole driver of all Raft instances.** No other code calls `step()`, `handle_timeout()`, or `propose()` on `Raft` instance in production.

2. **Every event must be followed by flush.** `Raft` state machine buffers all side effects internally. Skip flush → packets and timer commands silently lost.

3. **Raft instances are independent.** Processing event for `Shard #12` never touches `Shard #45`'s state.

4. **Group lifecycle controlled by Raft-Topology Bridge.** Actor does not decide when to create or remove groups. Only responds to `EnsureGroup` and `RemoveGroup` commands.

5. **Timer commands flow through scheduler.** Actor converts `TimerCommand<RaftTimer>` into `TickerCommand<RaftTimer>` (via `.into()`) and sends to shared ticker. Ticker fires `RaftTimeoutCallback` back into actor's mailbox.