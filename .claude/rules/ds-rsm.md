# DS-RSM (System-Level Architecture)

## Purpose

EastGuard uses Dynamically-Sharded Replicated State Machines (DS-RSM) instead of a monolithic metadata store or single controller quorum. Metadata is sharded by resource key and each shard forms a small Raft group among hosting nodes.

## Architecture

```
                   SWIM (UDP, membership + gossip)
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
           Node A      Node B      Node C
           ┌─────┐    ┌─────┐    ┌─────┐
           │Shard│    │Shard│    │Shard│
           │ #1  │◄──►│ #1  │◄──►│ #1  │   ← Raft group (TCP)
           │(L)  │    │(F)  │    │(F)  │
           ├─────┤    ├─────┤    ├─────┤
           │Shard│    │Shard│    │Shard│
           │ #2  │    │ #2  │    │ #2  │   ← another Raft group
           │(F)  │    │(L)  │    │(F)  │
           └─────┘    └─────┘    └─────┘
```

Each node runs `MultiRaftActor` multiplexing all its shard groups. Single actor, many state machines.

## Subsystem Hierarchy

DS-RSM is the top-level architecture. All other components are subsystems within it.

```
DS-RSM (this file: ds-rsm.md)
├── Membership Layer
│   ├── SwimActor / Swim           → swim.md
│   ├── Topology (hash ring)       → topology.md
│   └── SwimTransportActor (UDP)   → (covered in swim.md)
│
├── Consensus Layer
│   ├── MultiRaftActor / MultiRaft → raft-actor.md
│   ├── Raft (per shard group)     → raft.md
│   ├── MetadataStateMachine       → metadata-state-machine.md
│   └── RaftTransportActor (TCP)   → raft-transport.md
│
├── Timer Infrastructure
│   └── Ticker / Scheduler         → scheduler.md
│
└── Storage
    └── RocksDB key layout         → storage-layout.md
```

Membership drives consensus: SWIM detects node join/death → tells MultiRaftActor to create/remove groups and add/remove peers. Consensus drives application state: Raft commits log entries → MetadataStateMachine applies them.

## Component Ownership

| Component | Owns | Driven By |
|---|---|---|
| `SwimActor` | `Swim` state machine | UDP packets, timer callbacks |
| `MultiRaftActor` | `MultiRaft` → `HashMap<ShardGroupId, Raft>` | TCP packets, timer callbacks, SWIM membership events |
| `Raft` | `MetadataStateMachine` (per shard group) | Committed log entries |
| `Topology` | Consistent hash ring + shard leader map | SWIM membership events |
| `Ticker<T>` | Timer state for either SWIM or Raft | Real-time interval ticks |

## Routing

```
Client request (resource_key)
    │
    ▼
hash(resource_key) → ShardGroupId → Raft group
    │
    ▼
is_leader()? → propose(command) → replicate → commit → apply to MetadataStateMachine
```

`MultiRaftActor` handles routing internally. No separate broker or routing layer. `ShardGroupId::new(key)` is a pure hash — no topology query needed for routing.


## Invariants

1. **Each Raft instance is fully independent.** In DS-RSM, a node runs many Raft instances. They share no state — separate term, voted_for, log, commit_index, peers. `MultiRaftActor` maps `ShardGroupId → Raft`. Processing event for one shard never touches another's state.

2. **SWIM is sole membership authority.** Group lifecycle (create, remove) and peer changes (add, remove) are driven by SWIM membership events. `MultiRaftActor` does not decide when to create or remove groups — only responds to commands from `SwimActor`.

3. **Raft is transport-agnostic.** `Raft::peers` is `HashSet<NodeId>` — no addresses. State machine produces `OutboundRaftPacket { target: NodeId, rpc }`. Actor/transport layer resolves `NodeId → SocketAddr → Connection`.

4. **Sync-first design.** All protocol logic (`Swim`, `Raft`, `MetadataStateMachine`, `Ticker`, `GossipBuffer`, `Topology`) is purely synchronous. No async, no I/O, no channels. Side effects buffered internally and drained by owning actor after each event.

5. **Every event must be followed by flushing output buffers.** After every `step()`, `handle_timeout()`, or `propose()` call, the actor must drain `take_outbound()`, `take_timer_commands()`, `take_events()`, and `take_log_mutations()`. Skip flush → packets and timer commands silently lost.

6. **Timer seq values unique across all shard groups on a node.** `MultiRaft` uses `RaftTimerTokenGenerator` (monotonic wrapping counter) to assign globally unique timer seq values. Prevents cross-group timer collisions in shared `Ticker`.

7. **Dual transport separation.** SWIM uses UDP on `cluster_port` for failure detection and gossip. Raft uses TCP on `raft_port` for log replication and elections. Different protocols, different actors, different ports. Never mixed.

8. **Leader forwarding is max 1 hop.** `ProposeRequest.forwarded: bool` prevents re-forwarding. Non-leader forwards to leader once; if that also fails, error returned to client.

9. **Stale proposals are safe.** Split/merge/seal proposals that arrive after state has changed are rejected by `apply_*` precondition checks in MetadataStateMachine. Two leaders briefly coexisting (network partition) cannot cause inconsistency — only one proposal commits.

10. **No cross-shard coordination for single-topic operations.** Topic and all its ranges and segments live in one shard group. Every mutation is a single Raft log entry — atomic by construction. Cross-topic operations (e.g., list all topics) require scatter-gather.
