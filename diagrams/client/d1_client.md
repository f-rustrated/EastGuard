# Eastguard Client Layer

This spec defines two client-facing planes:

- Control Plane: topic lifecycle and cluster metadata (create topic, describe topic, list cluster members)
- Data Plane: message produce and consume (write a record, read records by offset, commit consumer position)

The goal is Kafka-style produce/consume over an Eastguard-native binary protocol. Kafka wire protocol
compatibility is deliberately out of scope for now but might be supported in the future.

Topics and their ranges/segments live in the **Raft-managed control plane** (`MetadataStateMachine`).
Consumer group state (membership, assignments, committed offsets) also goes there via new `MetadataCommand`
variants. The data plane (`DataPlaneActor`, WAL, segment files) stores only message payloads.

## Implementation Phases

**Phase 1 — Client-side offset tracking (current)**

Consumers carry their own read position. Every `Fetch` request includes an explicit `offset` field.
The server is stateless with respect to consumer position. In scope:

- Wire protocol (framing, `request_id`, out-of-order responses)
- Control Plane: `CreateTopic`, `DeleteTopic`, `ListTopics`, `DescribeTopic`
- Data Plane: `Produce`, `Fetch`, `ListOffsets`
- Admin: `DescribeCluster`, `ListTopicsWithStats`, `SplitRange`
- `TopicRoutingCache`, `NotLeader` redirect, `ClientHandler` dispatch

**Phase 2 — Server-side offset tracking (deferred)**

Adds consumer group coordination and durable offset commits. In scope:

- `ConsumerGroup` request variant: `JoinGroup`, `Heartbeat`, `LeaveGroup`
- Data Plane: `CommitOffset`, `FetchCommittedOffset`
- Coordinator, `MetadataStateMachineObserver` (consumer group impl), `ConsumerGroupMeta`
- Offset log design (dedicated segment via DataPlaneActor, rebuilt on coordinator failover)

Phase 1 clients remain wire-compatible with Phase 2 servers — the protocol is additive.

## Connection Model

Connections are persistent. One TCP connection carries many request/response cycles for the lifetime of the
client session. Multiple requests may be in-flight simultaneously (see Wire Protocol below). There is no
handshake or authentication in the initial implementation.

The existing `ClientStreamReader::handle_client_stream()` evolves into this loop.

## Wire Protocol

All frames use the same length-prefix framing already in place, extended with a `request_id`:

```
Request frame:  [len: u32][request_id: u64][ClientRequest payload: N bytes]
Response frame: [len: u32][request_id: u64][ClientResponse payload: N bytes]
```

- `request_id`: client-assigned, monotonically increasing per connection (wraps at `u64::MAX`).
- Responses carry back the same `request_id` — client matches responses to in-flight requests.
- Responses may arrive **out of order** relative to requests. The client must handle this.
- A single connection may have up to `max_in_flight` outstanding requests (configurable, default 256).
  Exceeding this limit: client blocks until a response arrives.

This replaces the current implicit one-at-a-time request/response model.

## Protocol - Request/Response Shape

Top-level dispatch enum (replaces `ConnectionRequests`):

```
ClientRequest:
  ControlPlane(ControlPlaneRequest)   -- Phase 1
  DataPlane(DataPlaneRequest)         -- Phase 1
  ConsumerGroup(ConsumerGroupRequest) -- Phase 2
  Admin(AdminRequest)                 -- Phase 1
```

Common error response variants (any handler can return these):

```
NotLeader { leader_addr: Option<SocketAddr>, retry_after_ms: Option<u64> }
  <- leader_addr: the current leader's client-facing address (if known)
  <- retry_after_ms: hint for election in progress. Not populated in v1 — client always
     applies exponential backoff regardless.

StaleGeneration
InternalError(String)
```

Client retry policy on `NotLeader`:

- Reconnect to `leader_addr` if provided; otherwise retry current node.
- Exponential backoff: base 50ms, multiplier 2x, jitter ±20%, configurable max timeout.
  Default max: 5 000ms.

## Server-Side Dispatch Architecture

The connection handler is split into a reader task and a writer task. A channel connects them.

```
TcpListener::accept()
        │
        ▼
    read loop (dedicated tokio task)
        │  reads frame → (request_id, ClientRequest)
        │  spawns one handler task per request
        │
        ├── handler task (request_id=1, ControlPlane::CreateTopic)  ─┐
        ├── handler task (request_id=2, DataPlane::Produce)          │ sends (request_id, response)
        ├── handler task (request_id=3, Admin::DescribeCluster)      │ to writer_tx channel
        ...                                                           │
        ▼                                                             │
    write loop (dedicated tokio task) <──────────────────────────────┘
        │  reads (request_id, response) from channel
        │  encodes and writes frame to TCP
```

`ClientHandler` — shared state cloned into each handler task:

```rust
struct ClientHandler {
    node_id: NodeId,
    swim_sender: SwimSender,
    raft_sender: RaftSender,
    data_plane_sender: DataPlaneSender,
    routing_cache: Arc<TopicRoutingCache>,   // per-node, shared across all connections
    writer_tx: mpsc::Sender<(u64, ClientResponse)>,
}
```

Each request variant dispatches to the appropriate sender:

- `ControlPlane` → `raft_sender` (Raft propose or metadata query)
- `DataPlane::Produce` → `routing_cache` lookup → `data_plane_sender`
- `DataPlane::Fetch` / `ListOffsets` → `data_plane_sender`
- `ConsumerGroup` → `raft_sender` (coordinator propose) + `routing_cache` *(Phase 2)*
- `Admin` → `swim_sender` + `raft_sender`

## Control Plane

All writes go through Raft consensus. The node receiving the request forwards it to `MultiRaftActor`
as a `Propose`. If this node is not the shard leader, it returns `NotLeader { leader_addr }` and the
client reconnects.

Read-only queries (`ListTopics`, `DescribeTopic`) are served from local `MetadataStateMachine` state —
no Raft proposal. Reads may be stale on followers; this is acceptable for the initial implementation.

### Protocols

**CreateTopic**

```
Request:  { name: String, retention_ms: u64, replication_factor: u8 }
Response: Ok | AlreadyExists | NotLeader
```

- Calling twice with the same topic name returns `AlreadyExists` (idempotent).

**DeleteTopic**

```
Request:  { name: String }
Response: Ok | TopicNotFound | NotLeader
```

**ListTopics**

```
Request:  {}
Response: { topics: Vec<TopicSummary { name: String, range_count: u32, state: TopicState }> }
```

- Returns only topics whose shard group is hosted on this node. No scatter-gather.
  A client wanting a cluster-wide view must query all nodes.

**DescribeTopic**

```
Request:  { name: String }
Response: TopicDetail {
    name: String,
    ranges: Vec<RangeDetail {
        range_id: u64,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
        active_segment_id: Option<u64>,
        state: RangeState,
    }>
} | TopicNotFound
```

## Data Plane

Data plane writes go to `DataPlaneActor` — an append-only WAL + segment file engine on the local disk.
No Raft involvement for message data.

**Segment leader vs. Raft leader:** These are different roles.

- The **Raft leader** for a shard group manages topic metadata.
- The **segment leader** (`replica_set[0]` in `SegmentMeta`) handles writes for a specific segment.

For `Produce`: the receiving node must be the segment leader. If not, it returns
`NotLeader { leader_addr: segment_leader_addr }` where `leader_addr` is resolved from the topic
routing cache → `replica_set[0]` → SWIM address lookup.

For `Fetch`: any node in `replica_set` can serve reads (followers track `commit_offset` piggybacked
on each `ReplicaAppend` and only serve up to it). If the receiving node is NOT in the `replica_set`,
it returns `NotLeader { leader_addr: replica_set[0] }`.

### Topic Routing Snapshot Cache

See "Topic Routing Cache" section for the concrete type. Used by all `DataPlane` and `ConsumerGroup`
handlers to resolve `topic_name + partition_key → range → segment → replica_set` without crossing
the actor boundary on every request.

### Protocols

**Produce**

```
Request:  { topic_name: String, partition_key: Vec<u8>, value: Vec<u8>,
            producer_id: u64, sequence_no: u64 }
Response: { offset: u64 } | NotLeader | TopicNotFound | RangeSplitting { ... }
```

`RangeSplitting { left_range_id: u64, right_range_id: u64, split_point: Vec<u8> }`:
The target range has been sealed by a split. The response includes the child range IDs and the
`split_point` so the client can immediately route without a metadata round-trip:

- `partition_key < split_point` → retry with `left_range_id`
- `partition_key >= split_point` → retry with `right_range_id`

If the child range's active segment is not yet assigned, the client applies exponential backoff
(configurable: base 50ms, 2x multiplier, configurable max). The max retry timeout defaults to
`produce_split_redirect_timeout_ms` (default: 5 000ms).

`(producer_id, sequence_no)` is an application-level idempotency key embedded in every record.
The broker does **not** deduplicate — this is an app-level primitive for exactly-once semantics.

**Fetch**

```
Request:  { topic_name: String, range_id: u64, offset: u64, max_bytes: u32 }
Response: { records: Vec<Record>, high_watermark: u64,
            sealed: bool, end_offset: Option<u64>,
            split_into: Option<[u64; 2]>, merged_into: Option<u64> }
         | NotLeader | OffsetOutOfRange
```

Reads from the segment file/cache starting at `offset`, up to `max_bytes`.
`high_watermark`: next offset to be written (one past the last available committed record).
If `offset == high_watermark`, `records` is empty — consumer polls again after a wait.

`sealed`, `end_offset`, `split_into`, `merged_into`: non-null when the range has been sealed.
Consumer uses these to navigate range transitions without a separate metadata query (see D4).

**ListOffsets**

```
Request:  { topic_name: String, range_id: u64 }
Response: { start_offset: u64, high_watermark: u64 } | NotLeader
```

**CommitOffset** *(Phase 2)*

```
Request:  { consumer_group: String, topic_name: String, range_id: u64,
            offset: u64, generation_id: u32 }
Response: Ok | StaleGeneration | NotLeader
```

Offset is Raft-proposed via `MetadataCommand::CommitConsumerOffset`. The receiving node must be the
Raft leader for the shard group that owns the topic. If not, returns `NotLeader`.

**FetchCommittedOffset** *(Phase 2)*

```
Request:  { consumer_group: String, topic_name: String, range_id: u64 }
Response: { offset: Option<u64> } | NotLeader
```

Read-only query against local `MetadataStateMachine`. Returns `None` if no offset has been committed
for this group/range yet.

## Topic Routing Cache

Shared per-node across all connections. Populated on first use per topic, invalidated on topology
changes (range split, merge).

```rust
pub struct TopicRoutingCache {
    inner: Arc<RwLock<HashMap<String, Arc<TopicRouting>>>>,
}

pub struct TopicRouting {
    pub topic_id: TopicId,
    pub shard_group_id: ShardGroupId,
    // Active ranges sorted by keyspace_start — binary searched on Produce.
    pub active_ranges: Vec<RangeRouting>,
    // All ranges (active + sealed) keyed by range_id — used by Fetch.
    pub all_ranges: HashMap<RangeId, RangeRouting>,
}

pub struct RangeRouting {
    pub range_id: RangeId,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub active_segment_id: Option<SegmentId>,
    pub replica_set: Vec<NodeId>,  // [0] = segment leader
}
```

**Invalidation**: `MetadataStateMachineObserver` (see Consumer Group section) fires on `RangeSplit`
and `RangeMerge`. The cache entry for the affected topic is removed. The next Produce/Fetch for that
topic triggers a rebuild via a new `MultiRaftActorCommand::GetTopicRouting { topic_name, reply }` query.

**Rebuild query path**: `GetTopicRouting` asks `MultiRaft` for the topic's active ranges and all
segments' replica sets from the local `MetadataStateMachine`. The result is cached until the next
invalidation.

## Admin API

Implemented early for testability. Admin handlers query local state only — no Raft proposals.

### Protocols

**DescribeCluster**

```
Request:  {}
Response: { nodes: Vec<NodeInfo { node_id: String, addr: SocketAddr, state: NodeState }> }

NodeState: Alive | Suspect | Dead
```

Sourced from SWIM membership table. `state` reflects the local SWIM view of each node.

**ListTopicsWithStats**

```
Request:  {}
Response: { topics: Vec<TopicStats { name: String, range_count: u32,
                                     total_records: u64, total_bytes: u64 }> }
```

- `total_records` / `total_bytes`: summed from `SegmentMeta` on the local shard groups only.

**SplitRange**

```
Request:  { topic_name: String, range_id: u64, split_point: Vec<u8> }
Response: Ok | NotLeader | InvalidSplitPoint
```

## Consumer Group *(Phase 2)*

Single-consumer end-to-end is proven first. Consumer group coordination is added as a layer on top.

Multiple consumer instances share a topic without duplicate processing. Instance A consumes ranges
1–3, instance B consumes ranges 4–6. When A dies, B takes over A's ranges starting from A's last
committed offset.

### Generation ID

`generation_id` is a monotonically incrementing counter on `ConsumerGroupMeta`. It increments on
every rebalance event:

- Member joins
- Member leaves (voluntary or crash-detected via heartbeat timeout)
- Range split or merge creates new range assignments

**Purpose — fencing stale consumers.** A consumer that missed a rebalance (network partition,
GC pause, etc.) still holds an old `generation_id`. Every `CommitOffset` and `Heartbeat` carries
the consumer's current `generation_id`. If it doesn't match the group's current `generation_id`:

- `CommitOffset` is rejected with `StaleGeneration`
- `HeartbeatResponse` carries `GroupEvent::StaleGeneration`

The consumer then calls `JoinGroup` again to get a fresh `generation_id` and updated assignments.
This prevents split-brain: a consumer that missed a rebalance cannot commit offsets for ranges it
no longer owns.

### Coordinator

The consumer group coordinator is the Raft leader of the shard group that owns the topic metadata.
Coordinator state (group membership, range assignments, `generation_id`, committed offsets) is stored
in `MetadataStateMachine` via Raft.

The coordinator registers as a `MetadataStateMachineObserver` and receives callbacks after each
committed state machine transition. On a `RangeSplit` or `RangeMerge` event, the coordinator
immediately runs the assignment algorithm to update affected members.

**Heartbeat timeout**: configurable via `consumer_heartbeat_timeout_ms` (default: 30 000ms).
The coordinator tracks `last_heartbeat: Instant` per member locally (not Raft-replicated — ephemeral,
rebuilt as members heartbeat to the new leader after failover). A periodic ticker (every 5s) checks
all members; members exceeding the timeout are declared dead and their ranges redistributed.

### MetadataStateMachineObserver

Registered with `MultiRaft`. Called after each `apply_committed_entries()` iteration.

The trait itself is Phase 1 (used by `TopicRoutingCache` for invalidation on `RangeSplit`/`RangeMerged`).
The consumer group coordinator implementation of this trait is Phase 2.

```rust
pub trait MetadataStateMachineObserver: Send + Sync {
    fn on_applied(&self, shard_group_id: ShardGroupId, event: &MetadataApplyEvent);
}

pub enum MetadataApplyEvent {
    TopicCreated { topic_id: TopicId, name: String },
    TopicDeleted { topic_id: TopicId },
    RangeSplit { topic_id: TopicId, parent: RangeId, left: RangeId, right: RangeId },
    RangeMerged { topic_id: TopicId, left: RangeId, right: RangeId, merged: RangeId },
    SegmentRolled { topic_id: TopicId, range_id: RangeId },
    ConsumerGroupUpdated { group_id: String, topic_id: TopicId },
}
```

`MultiRaft` holds `Option<Arc<dyn MetadataStateMachineObserver>>`. Set once at node startup before
the actor loop starts. The coordinator component (consumer group manager) implements this trait.

The `TopicRoutingCache` also implements `MetadataStateMachineObserver` to invalidate on
`RangeSplit` and `RangeMerged`. Both observers can be composed (e.g., a `Vec` of observers or a
fan-out wrapper).

### Consumer Group State

Stored in `MetadataStateMachine`. New types alongside existing topic/range/segment types:

```rust
pub struct ConsumerGroupMeta {
    pub group_id: String,
    pub topic_id: TopicId,
    pub generation_id: u32,
    // member_id -> assigned range IDs
    pub members: HashMap<String, Vec<RangeId>>,
    // committed offset per range (group-level, not per-member)
    pub committed_offsets: HashMap<RangeId, u64>,
}
```

New `MetadataCommand` variants:

```rust
JoinConsumerGroup {
group_id:  String,
topic_id:  TopicId,
member_id: String,
joined_at: u64,
}

LeaveConsumerGroup {
group_id:  String,
member_id: String,
}

AssignRanges {
group_id:      String,
topic_id:      TopicId,
assignments:   HashMap<String, Vec<RangeId> >,  // member_id -> ranges
generation_id: u32,
}

CommitConsumerOffset {
group_id:      String,
topic_id:      TopicId,
range_id:      RangeId,
offset:        u64,
generation_id: u32,
}
```

`CommitConsumerOffset` is validated against `ConsumerGroupMeta.generation_id` inside
`MetadataStateMachine::apply()`. Stale-generation commits are rejected with `MetadataError::StaleGeneration`.

### Protocols

**JoinGroup**

```
Request:  { group_id: String, topic_name: String, member_id: Option<String> }
Response: { member_id: String, generation_id: u32,
            assigned_ranges: Vec<{ range_id: u64, start_offset: u64 }> }
```

First join: `member_id = None`. Coordinator proposes `JoinConsumerGroup` with a new UUID member ID,
then `AssignRanges` (increments `generation_id`).
Rejoin (after `StaleGeneration`): pass existing `member_id`. Coordinator looks up existing assignments
or re-runs the algorithm if membership changed while the member was gone.

`start_offset`: the group's last committed offset for that range, or 0 if never committed.

**Heartbeat**

```
Request:  { group_id: String, member_id: String, generation_id: u32 }
Response: { events: Vec<GroupEvent> }
```

`GroupEvent` is one of:

- `AssignRange { range_id: u64, start_offset: u64 }`: start consuming from `start_offset`
- `RevokeRange { range_id: u64 }`: stop consuming immediately
- `StaleGeneration`: call `JoinGroup` again

Empty `events` = all is well, keep heartbeating.

**LeaveGroup**

```
Request: { group_id: String, member_id: String }
```

Fire-and-forget. Coordinator proposes `LeaveConsumerGroup` + `AssignRanges` (re-distributes
the leaving member's ranges to surviving members, increments `generation_id`).

### Assignment Algorithm

**On join**

1. If group has no members → assign all active ranges to the new member.
2. If group is stable → find member with most assigned ranges → steal one → assign to new member
   via `AssignRanges` proposal.
3. If all existing members have exactly one range and topic has only one active range →
   register new member with empty assignment. When the range auto-splits (coordinator gets
   `RangeSplit` callback), immediately run the steal algorithm and deliver `AssignRange` to the
   waiting member via next heartbeat.

**On leave / crash (heartbeat timeout)**

1. Dead member's ranges are distributed round-robin to surviving members (least-loaded first).
2. For each reassigned range, coordinator reads the last committed offset from
   `ConsumerGroupMeta.committed_offsets` and includes it in the `AssignRange` event.

**On RangeSplit**

Coordinator receives `MetadataApplyEvent::RangeSplit { parent, left, right }`.

1. Find the member that owned `parent`.
2. Revoke `parent` from that member via next heartbeat (`RevokeRange { range_id: parent }`).
3. Assign `left` to the original owner (continuity) and `right` to the least-loaded member.
   If only one member exists, assign both to that member.
4. `start_offset` for both children: 0 (children always start fresh).
5. Increment `generation_id` and propose `AssignRanges`.

**On RangeMerge**

Coordinator receives `MetadataApplyEvent::RangeMerged { left, right, merged }`.

1. `left` and `right` may be owned by different members. Revoke both via next heartbeat.
2. Assign `merged` to whichever member had fewer total ranges before (or the owner of `left`
   as tiebreaker).
3. `start_offset` for `merged`: 0 (merged range always starts fresh at offset 0).
4. Members draining source ranges: they continue consuming sealed `left`/`right` to their
   `end_offset`, then transition to `merged` starting at 0.
5. Increment `generation_id` and propose `AssignRanges`.

**Algorithm replaceability**

The assignment logic is behind a trait. Round-robin leave distribution and work-stealing joins are
the default implementation. Swapping to a different strategy (rack-aware, sticky) requires only a
new impl, no protocol changes.

**Exactly-Once Semantics**

Delivery is at-least-once. When a range moves from consumer A to consumer B:

- B starts from A's last committed offset
- Records A fetched but did not commit are re-delivered to B
- `generation_id` fencing ensures A cannot commit after losing the range

Applications requiring exactly-once deduplicate using `record_id = (producer_id, sequence_no)`
embedded in every record. The broker does not deduplicate.

## Consumer Offset Storage *(Phase 2)*

Committed offsets are Raft-replicated via `MetadataCommand::CommitConsumerOffset`. This means:

- Offsets survive coordinator (Raft leader) failover — the new leader picks up from persisted state.
- `CommitOffset` latency = Raft round-trip (~1s typical at the current heartbeat interval).
  For high-throughput consumers, batch offset commits (commit every N records or every T ms).
- The `MetadataStateMachine` stores `committed_offsets: HashMap<RangeId, u64>` inside
  `ConsumerGroupMeta`. The offset is per-group per-range (not per-member) — when a range is
  reassigned, the new owner picks up from the group's last committed position.

**Commit flow**:

```
Consumer → CommitOffset(group, topic, range, offset, generation_id)
    → Server: validate generation_id matches ConsumerGroupMeta.generation_id
    → Server: Raft propose CommitConsumerOffset
    → On commit: MetadataStateMachine updates committed_offsets[range_id] = offset
    → Server: respond Ok
```

**Fetch committed offset**: read-only query against local `MetadataStateMachine`. Any node can serve
it (may be stale on followers, acceptable for consumer recovery). Returns `None` if no offset has
been committed for this group/range.

## Client Routing

Clients connect to any seed node. The server handles redirect via `NotLeader { leader_addr }`.

**Client-side routing cache**

- `HashMap<TopicName, TopicPartitionInfo>` is populated from `DescribeTopic` responses.
- Invalidates on `NotLeader` for that topic → fresh `DescribeTopic` before retry.
- Retry policy: up to 3 attempts, 50ms base backoff, 2x multiplier, ±20% jitter.
  If all 3 fail, surface error to caller.
