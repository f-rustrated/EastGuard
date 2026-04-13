# Raft State Machine

## Purpose

`Raft` is the consensus state machine for DS-RSM (Dynamically-Sharded Replicated State Machine). Each shard group on a node runs its own independent `Raft` instance. It handles leader election, log replication, and commit tracking. Like `Swim`, it is purely synchronous with no I/O.

## DS-RSM Context

EastGuard does not use a monolithic metadata store or a single controller quorum. Instead, metadata is sharded by resource key (e.g., Topic ID), and each shard forms a small Raft group among the nodes that host it. This means:

- A single node participates in **many** Raft groups simultaneously (some as leader, others as follower).
- Each `Raft` instance has its own term, voted_for, log, and commit_index -- fully independent.
- `ShardRaftManager` (future) multiplexes all groups through a single `RaftActor`.
- Log storage is per-instance (`MemLog`) now; will become a shared store keyed by `(shard_id, index)` when RocksDB is added.

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
```

## Key Types

| Type | File | Description |
|---|---|---|
| `Raft` | `src/raft/state.rs` | Core state machine. Holds term, role, log, peers, commit_index. |
| `Role` | `src/raft/state.rs` | `Follower`, `Candidate { votes_received }`, `Leader` |
| `PeerState` | `src/raft/state.rs` | Leader-only. `next_index` (guess) and `match_index` (confirmed truth). |
| `MemLog` | `src/raft/log.rs` | In-memory log store. 1-based indexing. |
| `LogEntry` | `src/raft/messages.rs` | `{ term, index, command }` |
| `RaftCommand` | `src/raft/messages.rs` | `Noop` for now. Will grow: `CreateTopic`, `AssignRange`, `MoveShard`. |
| `ProposeError` | `src/raft/messages.rs` | `NotLeader` (returned by `propose()` when not the leader). |
| `RaftRpc` | `src/raft/messages.rs` | Enum: `RequestVote`, `RequestVoteResponse`, `AppendEntries`, `AppendEntriesResponse`. |
| `OutboundRaftPacket` | `src/raft/messages.rs` | `{ target: NodeId, rpc: RaftRpc }`. Transport-agnostic — actor resolves NodeId to connection. |
| `RaftTimer` | `src/raft/messages.rs` | Implements `TTimer`. Two kinds: `Election`, `Heartbeat`. |
| `RaftTimeoutCallback` | `src/raft/messages.rs` | `ElectionTimeout` (default), `HeartbeatTimeout`. |

## Timer Model

Raft uses fixed well-known seq values instead of a rolling counter (unlike SWIM's `seq_counter`), because only two timers are ever active:

| Constant | Value | Purpose |
|---|---|---|
| `ELECTION_TIMER_SEQ` | 0 | Election timeout (Follower/Candidate only) |
| `HEARTBEAT_TIMER_SEQ` | 1 | Heartbeat interval (Leader only) |

- `reset_election_timer()`: cancels seq 0, then sets seq 0 with jitter.
- `schedule_heartbeat_timer()`: sets seq 1.
- `cancel_all_timers()`: cancels both seq 0 and seq 1.

Timer durations (at 100ms tick period):
- Election: 5s base + jitter (configurable via `election_jitter`)
- Heartbeat: 1s

These are relaxed compared to typical Raft implementations because DS-RSM manages metadata (topic assignments, range ownership), not data-plane traffic. Consistency matters more than heartbeat latency. At 600 nodes × 256 vnodes, each node hosts ~768 shard groups — relaxed intervals keep timer load manageable (~256 leader heartbeat callbacks/sec).

## Role Safety

`handle_timeout` guards on role:
- `ElectionTimeout` is ignored if the node is a Leader.
- `HeartbeatTimeout` is ignored if the node is a Follower or Candidate.

All `step()` handlers accept RPCs from any role (per Raft spec §5.1), but role-specific actions (counting votes, tracking peer state) guard internally.

## State Transitions

```
Follower ──[ElectionTimeout]──> Candidate ──[majority votes]──> Leader
    ^                               |                              |
    |                               |                              |
    └──[higher term from any RPC]───┴──[higher term from any RPC]──┘
```

- **Follower → Candidate**: Election timeout fires. Increments term, votes for self, sends `RequestVote` to all peers.
- **Candidate → Leader**: Receives majority of votes. Initializes `peer_states`, cancels election timer, starts heartbeat timer, sends initial heartbeats.
- **Any → Follower**: Receives RPC with higher term. Resets term, clears `voted_for`, cancels timers, starts election timer.
- **Candidate → Follower**: Receives `AppendEntries` with same term (another node already won).
- **Single-node**: Candidate with no peers immediately becomes Leader.

## Commit Semantics

- `commit_index` advances when a log entry from the **current term** is replicated on a majority (Figure 8 safety rule -- cannot directly commit old-term entries).
- `last_applied` (future, Phase 4.5) will track the last entry applied to the per-shard state machine.
- The apply loop will drain `last_applied+1..=commit_index` after each commit advancement.

## Log Replication

Leader maintains per-peer `PeerState`:
- `next_index`: initialized to `last_log_index + 1` (optimistic). Decremented on rejection.
- `match_index`: initialized to 0 (conservative). Updated on successful `AppendEntriesResponse`.

They converge to `next_index = match_index + 1` once the peer is caught up. The initial probing phase is designed to have them diverge.

## Invariants

1. **Raft is purely synchronous.** No async, no I/O, no channels. All side effects are buffered in `pending_outbound` and `pending_timer_commands`.

2. **Each Raft instance is independent.** In DS-RSM, a node runs many Raft instances. They share no state. The `ShardRaftManager` (future) maps `ShardGroupId → Raft`.

3. **Every event must be followed by draining output buffers.** The actor layer must call `take_outbound()` and `take_timer_commands()` after every `step()`, `handle_timeout()`, or `propose()` call.

4. **Only the leader can propose.** `propose()` returns `Err(ProposeError::NotLeader)` if called on a follower or candidate.

5. **At most one leader per term.** Enforced by vote dedup (`voted_for`) and log-up-to-date check (§5.4.1).

6. **Dead entries from old terms are not directly committed.** The leader only commits entries matching `current_term`. Preceding entries are implicitly committed once a current-term entry is committed.
