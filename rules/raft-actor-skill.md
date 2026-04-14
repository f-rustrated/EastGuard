# Raft Actor (DS-RSM)

## Purpose

`RaftActor` is the async boundary that drives multiple `Raft` state machines -- one per shard group on this node. It follows the same pattern as `SwimActor`: receives commands from a mailbox, feeds them into the synchronous state machines, and flushes all resulting side effects (outbound packets and timer commands).

The actor itself contains no consensus logic. All decisions live in `Raft`.

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

**Single actor, multiple state machines:** Unlike spawning one task per shard group, a single `RaftActor` multiplexes all groups. This keeps lifecycle management simple and avoids thousands of tokio tasks as shard count grows. If contention becomes an issue, this can be split later.

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
| `EnsureGroup` | Raft-Topology Bridge | Create `Raft` instance if not exists and this node is a member |
| `RemoveGroup` | Raft-Topology Bridge | Shut down and remove `Raft` instance |

## Flush Protocol

After every command, the actor must flush **all** groups that were touched:

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

Each `Raft` instance uses fixed seq values (`ELECTION_TIMER_SEQ = 0`, `HEARTBEAT_TIMER_SEQ = 1`). Since multiple groups share the same scheduler, the actor must **namespace** these seqs to avoid collisions:

```
effective_seq = (shard_group_id_hash << 2) | local_seq
```

This ensures `Shard #12`'s election timer doesn't cancel `Shard #45`'s election timer.

## Transport: TCP, not UDP

Raft RPCs go over TCP (reliable, ordered delivery). The `RaftTransportActor` listens on a dedicated `raft_port`. Each packet is tagged with `ShardGroupId` so the transport layer can route to the correct group.

This is separate from SWIM's UDP transport (`SwimTransportActor` on `cluster_port`).

## Leader Change Detection

The actor monitors `Raft::role()` after each event. When a group's role transitions to/from `Leader`, the actor emits a `ShardLeaderEvent` for SWIM gossip:

```rust
let was_leader = old_role == Role::Leader;
let is_leader = raft.is_leader();
if was_leader != is_leader {
    // Emit ShardLeaderEvent to SWIM gossip layer
}
```

This is the bridge that connects Raft elections to SWIM's `ShardLeader` piggybacking (#35-#38).

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

Both actors share the same `Ticker`-based scheduler (via `run_scheduling_actor`), but use different timer types (`SwimTimer` vs `RaftTimer`). Timer seq namespacing prevents collisions.

## Peer Resolution: NodeId → Connection

The `Raft` state machine is fully transport-agnostic. It identifies peers by `NodeId` only — no `SocketAddr`(or the like) in the state machine. The resolution chain:

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

- `Raft::peers` is a `HashSet<NodeId>` — no addresses. The state machine produces `OutboundRaftPacket { target: NodeId, rpc }`. The actor/transport layer resolves `NodeId → SocketAddr → Connection`.

- The connection pool (`HashMap<NodeId, RaftWriter>`) lives in `RaftTransportActor`. Connections are persistent and bidirectional — one TCP connection per peer pair, split into `RaftReader`/`RaftWriter` halves.

- **Handshake**: On connect, the initiator sends its `NodeId` (length-prefixed bincode). The acceptor reads it to key the write half. This happens before any Raft RPCs.

- **Conflict resolution**: Either side can initiate a connection. On simultaneous connect, the connection initiated by the **lower `NodeId`** wins. The acceptor checks: if a writer for the peer already exists and `peer_id > self.node_id`, the incoming connection is dropped (we initiated as the lower NodeId, so ours takes precedence).

- SWIM is the authoritative source of `NodeId → SocketAddr` (via `SwimQueryCommand::ResolveAddress`). When a node's address changes, SWIM detects it via gossip. The bridge recreates affected shard groups.

- For ConfChanges (adding/removing a peer mid-term without destroying the group), `Raft` will need `add_peer(NodeId)` / `remove_peer(NodeId)` methods. Until then, membership changes are handled by destroying and recreating the `Raft` instance via `RemoveGroup` + `EnsureGroup`.

## Invariants

1. **The actor is the sole driver of all Raft instances.** No other code calls `step()`, `handle_timeout()`, or `propose()` on a `Raft` instance in production.

2. **Every event must be followed by a flush.** The `Raft` state machine buffers all side effects internally. If you skip the flush, packets and timer commands are silently lost.

3. **Raft instances are independent.** Processing an event for `Shard #12` never touches `Shard #45`'s state.

4. **Group lifecycle is controlled by the Raft-Topology Bridge.** The actor does not decide when to create or remove groups. It only responds to `EnsureGroup` and `RemoveGroup` commands.

5. **Timer commands flow through the scheduler.** The actor converts `TimerCommand<RaftTimer>` into `TickerCommand<RaftTimer>` (via `.into()`) and sends it to the shared ticker. The ticker fires `RaftTimeoutCallback` back into the actor's mailbox.
