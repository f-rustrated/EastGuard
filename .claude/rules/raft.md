# Raft State Machine

## Purpose

`Raft` = consensus state machine for DS-RSM (Dynamically-Sharded Replicated State Machine). Each shard group on node runs own independent `Raft` instance. Handles leader election, log replication, commit tracking. 

## DS-RSM Context

EastGuard not use monolithic metadata store or single controller quorum. Metadata sharded by resource key (e.g., Topic ID), each shard forms small Raft group among hosting nodes. Means:

- Single node participates in **many** Raft groups simultaneously (some leader, others follower).
- Each `Raft` instance has own term, voted_for, log, commit_index — fully independent.
- `MultiRaftActor` multiplexes all groups through `HashMap<ShardGroupId, Raft>`.
- Log storage backed by shared RocksDB instance, keyed by `(shard_group_id, key_type, index)`. See `diagrams/metadata-management/mental-model.md` § "Storage layout" for the full key scheme.

## Architecture

```
Raft (pure sync state machine, one per shard group)
    |
    |-- step(src, rpc)          <-- inbound RPC (RequestVote, AppendEntries, responses)
    |-- handle_timeout(event)   <-- timer callback (ElectionTimeout, HeartbeatTimeout)
    |-- propose(command)        <-- client proposal (leader only, returns Result)
    |
    |-- take_outbound()         --> Vec<OutboundRaftPacket>   (drain to transport)
    |-- take_timer_commands()   --> Vec<TimerCommand<RaftTimer>> (drain to scheduler)
    |
    |-- is_leader()             --> bool
    |-- has_peer(node_id)       --> bool
    |-- current_leader()        --> Option<&NodeId>
    |-- peers_count()           --> usize
```

## Key Types

| Type | Description |
|---|---|
| `Raft` | Core state machine. Holds term, role, log, peers, commit_index, last_applied. |
| `Role` | `Follower`, `Candidate { votes_received }`, `Leader` |
| `PeerState` | Leader-only. `next_index` (guess) and `match_index` (confirmed truth). |
| `MemLog` | In-memory log store. 1-based indexing. |
| `LogEntry` | `{ term, index, command }` |
| `RaftCommand` | `Noop`, `RemovePeer(NodeId)`, `AddPeer(NodeId)`. Will grow: `CreateTopic`, `AssignRange`, `MoveShard`. |
| `ProposeError` | `NotLeader` (returned by `propose()` when not leader). |
| `RaftRpc` | Enum: `RequestVote`, `RequestVoteResponse`, `AppendEntries`, `AppendEntriesResponse`. |
| `OutboundRaftPacket` | `{ target: NodeId, rpc: RaftRpc }`. Transport-agnostic — actor resolves NodeId to connection. |
| `RaftTimer` | Implements `TTimer`. Two kinds: `Election`, `Heartbeat`. |
| `RaftTimeoutCallback` | `ElectionTimeout` (default), `HeartbeatTimeout`. |

## Timer Model

Raft uses fixed well-known seq values instead of rolling counter (unlike SWIM's `seq_counter`), because only two timers ever active:

| Constant | Value | Purpose |
|---|---|---|
| `ELECTION_TIMER_SEQ` | 0 | Election timeout (Follower/Candidate only) |
| `HEARTBEAT_TIMER_SEQ` | 1 | Heartbeat interval (Leader only) |

- `reset_election_timer()`: cancels seq 0, sets seq 0 with jitter.
- `schedule_heartbeat_timer()`: sets seq 1.
- `cancel_all_timers()`: cancels both seq 0 and seq 1.

Timer durations (at 100ms tick period):
- Election: 5s base + jitter (configurable via `election_jitter`)
- Heartbeat: 1s

Relaxed compared to typical Raft — DS-RSM manages metadata (topic assignments, range ownership), not data-plane traffic. Consistency matters more than heartbeat latency. At 600 nodes × 256 vnodes, each node hosts ~768 shard groups — relaxed intervals keep timer load manageable (~256 leader heartbeat callbacks/sec).

## Role Safety

`handle_timeout` guards on role:
- `ElectionTimeout` ignored if node is Leader.
- `HeartbeatTimeout` ignored if node is Follower or Candidate.

All `step()` handlers accept RPCs from any role (per Raft spec §5.1), but role-specific actions (counting votes, tracking peer state) guard internally.

## State Transitions

```
Follower ──[ElectionTimeout]──> Candidate ──[majority votes]──> Leader
    ^                               |                              |
    |                               |                              |
    └──[higher term from any RPC]───┴──[higher term from any RPC]──┘
```

- **Follower → Candidate**: Election timeout fires. Increments term, votes for self, sends `RequestVote` to all peers.
- **Candidate → Leader**: Receives majority votes. Initializes `peer_states`, cancels election timer, starts heartbeat timer, sends initial heartbeats.
- **Any → Follower**: Receives RPC with higher term. Resets term, clears `voted_for`, cancels timers, starts election timer.
- **Candidate → Follower**: Receives `AppendEntries` with same term (another node already won).
- **Single-node**: Candidate with no peers immediately becomes Leader.

## Commit Semantics

- `commit_index` advances when log entry from **current term** replicated on majority (Figure 8 safety rule — cannot directly commit old-term entries).
- `last_applied` tracks last entry applied. After each commit_index advancement, `apply_committed_entries()` drains `last_applied+1..=commit_index`.
- **Single-node clusters**: `try_advance_commit_index()` called after `propose()` and `become_leader()` — no peers to ack, quorum=1 (self), so entries commit immediately.

## ConfChange (Membership Changes)

- `RaftCommand::RemovePeer(NodeId)` — removes peer from `self.peers` and `self.peer_states` on commit. Proposed by leader via `HandleNodeDeath` (sent by SwimActor on node death).
- `RaftCommand::AddPeer(NodeId)` — inserts into `self.peers` on commit. Leader initializes `PeerState` for the new peer. Skips if target is self (self never in peers). Proposed by leader via `HandleNodeJoin` (sent by SwimActor on node join).
- ConfChange is Raft-internal — modifies the peer set, not the application state machine. Handled in `apply_committed_entries()`, separate from Phase 4's application apply path.

## Log Replication

Leader maintains per-peer `PeerState`:
- `next_index`: initialized to `last_log_index + 1` (optimistic). Decremented on rejection.
- `match_index`: initialized to 0 (conservative). Updated on successful `AppendEntriesResponse`.

Converge to `next_index = match_index + 1` once peer caught up. Initial probing phase designed to have them diverge.

## Invariants

1. **Only the leader can propose.** `propose()` returns `Err(ProposeError::NotLeader)` if called on a follower or candidate. Followers may apply, never originate.

2. **At most one leader per term.** Enforced by vote deduplication (`voted_for`) and the log-up-to-date check (§5.4.1). Two leaders in the same term would commit conflicting entries — Raft's primary safety violation.

3. **A leader commits only entries from its own term.** Entries from prior terms become committed implicitly when a current-term entry is committed (Figure 8). Direct commit of an old-term entry can be retroactively overwritten by a yet-older leader's truncation — a safety violation.

4. **Apply is gated by both commit and durability.** `apply_committed_entries` advances `last_applied_index` only up to `min(commit_index, stabled_index)`. An entry is never applied to state on a replica that has not also persisted it. Without this, a crash after apply but before persistence would lose already-observable state.

5. **`last_applied_index ≤ commit_index ≤ stabled_index ≤ log_last_index`** at all times. Maintained by: the apply-gate (#4), `stabled_index` advancing only after a successful flush, `commit_index` only set inside the quorum-ack check, and entries appearing in the log before being indexable.

6. **The peer set mutates only via apply of committed `AddPeer` / `RemovePeer` entries.** No external code path mutates `peers`. The single bootstrap exception: initial peers are seeded into the constructor when the group is created. Direct mutation outside this discipline lets replicas disagree on quorum size at the same log index → split-brain commit.

7. **`peer_states` exists only on the leader.** Initialized in `become_leader` for every peer; cleared in `step_down`. Followers carry an empty `peer_states`. Apply-time `AddPeer` / `RemovePeer` keep `peer_states` in sync with `peers` *only when this replica is leader* at apply time.

8. **Apply is deterministic across replicas.** Given the same committed log, every replica produces the same `peers`, the same `MetadataStateMachine`, the same `last_applied_index`. This is the foundation of the replicated state machine — anything that breaks it (originating mutations outside apply, non-deterministic apply logic) breaks consensus.