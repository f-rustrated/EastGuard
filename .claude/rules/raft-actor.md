# Raft Actor (DS-RSM)

## Purpose

`MultiRaftActor` is async boundary driving multiple `Raft` state machines -- one per shard group on this node. It receives commands from mailbox, feeds into synchronous state machines, flushes all side effects (outbound packets and timer commands).

Actor contains no consensus logic. All decisions live in `Raft`.


**Single actor, multiple state machines:** Instead of spawning one task per shard group, single `MultiRaftActor` multiplexes all groups. Keeps lifecycle management simple, avoids thousands of tokio tasks as shard count grows. Can split later if contention becomes issue.

## Command Types

```rust
pub enum RaftCommand {
    InboundRaftRpc {
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
    HandleNodeDeath(NodeId),
    HandleNodeJoin {
        new_node_id: NodeId,
        affected_groups: Vec<ShardGroup>,
    },
}
```

## Message Flow

| Variant | Source | Action |
|---|---|---|
| `InboundRaftRpc` | Transport layer (TCP) | Lookup `Raft` by `shard_group_id`, call `raft.step(from, rpc)` |
| `Timeout` | Ticker (timer expiry) | Extract `shard_group_id` from callback, call `raft.handle_timeout(cb)` |
| `EnsureGroup` | SwimActor / HandleNodeJoin | Create `Raft` instance if not exists and this node is member |
| `RemoveGroup` | External | Shut down and remove `Raft` instance |
| `GetLeader` | External callers | Read-only query, returns `Option<NodeId>` via oneshot |
| `Propose` | Client/MetadataStateMachine | Lookup `Raft` by `shard_group_id`, call `raft.propose(command)`, reply via oneshot |
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
SwimActor (UDP, membership) ──raft_tx──> MultiRaftActor (TCP, consensus)
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

SwimActor sends membership-driven commands directly to MultiRaftActor — no intermediate bridge. Topology queries are local (`state.topology.shard_groups_for_node()`).

## Peer Resolution: NodeId → Connection

`Raft` state machine is fully transport-agnostic. Identifies peers by `NodeId` only — no `SocketAddr`(or like) in state machine. Resolution chain:

```
SWIM membership table          (NodeId → SocketAddr, maintained by gossip)
       |
       v
Topology / hash ring           (key → ShardGroup { members: Vec<NodeId> })
       |
       v
SwimActor                      (sends HandleNodeDeath/HandleNodeJoin to MultiRaftActor)
       |
       v
MultiRaftActor                      (maintains NodeId → Connection pool)
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

1. **Group lifecycle commands originate only from `SwimActor`.** `MultiRaftActor` does not decide when to create or remove groups, nor when to add or remove peers — it only responds to `EnsureGroup`, `RemoveGroup`, `HandleNodeDeath`, `HandleNodeJoin`. If two parts of the system could each initiate group lifecycle, races would let a group be removed by one path while the other is adding peers to it.

2. **On `LeaderChange → self`, the new leader reconciles against SWIM.** Actor snapshots SWIM's live member set and proposes `RemovePeer` for every current peer not in that set. Without this, SWIM facts about deaths that occurred while no leader existed (typically: the previous leader was one of the dead) never become Raft truth — the events have already been disseminated by SWIM and won't re-fire.

3. **`MetadataCommitted` is dispatched only by the current leader.** During event drain, if the replica is no longer leader, its `MetadataCommitted` events are discarded (the entry still applies — only the outbound dispatch is suppressed). Otherwise every replica would emit duplicate `SegmentAssignment` / `SealResponse` to the data plane, and a former leader would dispatch entries it can no longer be authoritative about.

4. **`pending_seals` is bounded.** Cleared in two places: (a) on observed `LogMutation::TruncateFrom(idx)`, entries with `log_index ≥ idx` are dropped — the new leader may have truncated those proposals; (b) when `!raft.is_leader()` at drain time, all entries for that group are dropped — the `SealResponse` will not come from this replica. Without this, unbounded growth as leaders churn.

5. **`flush` terminates within a bounded number of rounds.** `MultiRaftActor::flush` loops `store.flush()` up to `MAX_FLUSH_ROUNDS = 8` because `route_event` may call back into the store (reconciliation proposes new `RemovePeer` entries). Bounded so a feedback bug cannot deadlock the actor. Steady-state shape is ≤ 2 rounds.

6. **A leader-crash seal's end is recovered before the roll, as the `min` of survivors' durable extents.** When an active segment's sole dead member is its write leader (`replica_set[0]`), the coordinator does *not* roll immediately. It gathers each surviving replica's durable extent (`SealBoundaryQuery` → `SealBoundaryReport`) and seals at their `min` — the highest offset present on every survivor, hence committed (commit requires an all-replica ack). Only then does the `RollSegment` commit, so the successor opens at `min+1` (continuous — preserves `metadata-state-machine.md` #8) and the sealed segment is immediately known-end (repairable by the D5 `ReassignSegment` path). A missing survivor after `GATHER_ATTEMPTS` re-drives, or a multi-death, falls back to the unknown-end (`None`) roll — strictly no worse than no recovery. Candidates are found in `reconcile_segments`' single active-segment walk (`only_leader_dead`) and stashed for the actor to drain (`take_leaderless_segments`); multi-death and follower-only deaths are not candidates and roll there directly. The gather is idempotent: a proposed roll is held until it applies and is pruned, so a not-yet-applied roll can't restart it, and a takeover re-initiates on the new leader. The `SealEndRecovery` subsystem is bounded like `pending_seals` (#4): a deposed leader drops its gathers at the same drain-time `!is_leader` checkpoint (the ring-check that would otherwise prune is leader-gated and stops firing on step-down).

7. **The recovered segment is led by the most-complete survivor.** The new `replica_set[0]` is the survivor with the highest durable extent (`argmax`, ties broken by lowest `NodeId`) — a clean "most-caught-up replica wins" election, read from the same gather. The choice is the leader's and is committed via `RollSegment.new_replica_set` ordering, so apply stays deterministic (a leader-only *decision*, identical *apply* on every replica — cf. `metadata-state-machine.md` #16).

8. **Boundary recovery reads durable extents, never the notified commit cursor.** A `SealBoundaryReport` carries `next_entry_id - 1` (`SegmentTracker::durable_end_entry_id`), advanced only after `wal.flush_batch()` succeeds — distinct from and `≥` the *notified* `committed_entry_id`. A survivor's notified commit lags its durable extent (the leader acks the producer before propagating the commit notification), so trusting the cursor could drop an already-acked entry. `None` means the survivor holds nothing ⇒ nothing was committed ⇒ the segment seals empty.

9. **Sealed-segment catch-up is re-driven until confirmed.** The `CatchUpAssignment` a committed `ReassignSegment` dispatches (via the #3 drain) is one-shot and fire-and-forget. So on applying the reassignment the leader seeds a per-segment in-repair entry (the `CatchUpRepairs` tracker, leader-only — gated on role at apply like `peer_states` in `apply_add_peer`) and re-drives `CatchUpAssignment` to every not-yet-confirmed member on each heartbeat sweep (`maybe_redrive_catch_ups`, beside the active-segment `maybe_redrive_segment_assignments`). A member clears itself with a `CatchUpAck` once it holds the segment through `sealed_end`; the entry is pruned when all members confirm, superseded when the segment is re-reassigned, and dropped on step-down (leader-volatile, cleared with `confirmed_assignment`). `CatchUpAck` is idempotent and a full-match member re-confirms on re-drive (`data_plane/state.rs`), so no single dropped message strands a repair. A re-drive also rescues an *in-flight* receive that has stalled (dead source or dropped chunk): the receiver re-requests the remainder from a freshly-picked source rather than no-op'ing on the duplicate assignment, and its chunk handler appends only the strictly-next entry so a resume overlap can't inflate the verified prefix past `sealed_end`. On takeover the new leader re-seeds the tracker for every known-end sealed segment it leads (`reseed_catch_up`, from `reconcile_on_leadership_change`), since the tracker is leader-volatile — so a repair in flight at a leadership change is re-driven, not stranded (already-complete members full-match-ack cheaply; the re-drive of all sealed segments is the cost). This is a *rule*, not an invariant (it is a liveness guarantee over time, not a snapshot predicate): no `assert_invariants` entry.

10. **An under-replicated sealed segment is re-filled toward RF when the ring can supply a member.** `reconcile_segments` (`raft.md`) is *death-triggered* — it repairs only a segment that names a non-live member. When a death shrinks a segment to its survivors with no live replacement then available (`compute_replacement_replica_set` drops the dead member and targets the *shrunk* size, not the configured RF), the result has **no dead member** and is never revisited — a permanent, silent under-replication, one death from data loss and invisible to the death scan. `Raft::refill_under_replicated_segments` closes this: run on the periodic ring check (and takeover) from `reconcile`, it finds known-end sealed segments with `replica_set.len() < replication_factor` and proposes a `ReassignSegment` adding ring members (`ring_replacements_for`) up toward RF. It is the *capacity-return* counterpart to the death-triggered repair — when a node rejoins (or any node joins), the next ring check grows the segments it can host back toward RF, and the added member re-replicates via the #9 catch-up path: **zero-transfer if it already holds the data** (a restarted node reclaiming its own segments — the D5 recovery "lottery"). Bounded and idempotent: scoped to `len < rf`, so a segment at full RF is never touched (re-fill, **not** rebalance — ordinary ring drift can't churn well-replicated segments); additions are ring members only, keeping placement consistent with the hash ring; `ReassignSegment` on an unchanged set is a `Noop` (`metadata-state-machine.md` #21), and once the reassign commits the segment is no longer under-replicated, so it isn't re-proposed (steady-state proposal cost zero). Like #9 this is a *rule*, not an invariant — a liveness guarantee over time (eventually, on the next ring check), not a snapshot predicate (a segment is legitimately under-replicated between a death and the next check): no `assert_invariants` entry.