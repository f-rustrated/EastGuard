# Phase D6: Produce/Consume Client API

**Goal:** End-to-end path from client produce/consume through to storage engine. Sessionized streaming protocol with pipelining and windowing. DataPlaneActor (node-level) routes client sessions to SegmentActors (per-segment, spawned tasks) that handle storage, replication, and lifecycle.

**Depends on:** Phase D4 (consumer range tracking), Phase D5 (crash recovery).

---

## Produce Protocol

Sessionized and streaming (following Northguard's approach):

1. Producer opens TCP connection, sends handshake with stream ID
2. Broker responds with initial window size (flow control)
3. Producer sends `Append { stream_id, seq, records }` — multiple in-flight without waiting for ack (pipelining)
4. Broker acks committed records: `Ack { ack_seq, updated_window }` — M acks for N appends (batched)
5. Broker only acks records that have been fsync'd and replicated (D2)

Windowing prevents producer from overwhelming the broker. Broker adjusts window based on backpressure from disk/replication.

## DataPlaneActor and SegmentActor

Two-level architecture (see D1 "Storage Actor Architecture" for full details):

**DataPlaneActor** (one per node) — owns the shared WAL, maintains a segment registry (`HashMap<(ShardGroupId, RangeId, SegmentId), SegmentSender>`), and routes all client and replication traffic to the correct SegmentActor. Spawns/stops SegmentActors based on `SegmentAssignment`, self-authorizing `ReplicaAppend`, and node startup recovery (D5). Follows the existing actor pattern — `DataPlaneSender` typed wrapper, tokio task, mailbox-driven event loop.

**SegmentActor** (one spawned tokio task per segment) — owns cache, checkpoint, read serving, and replication for one segment. Each SegmentActor owns a sync state machine (cache, offset tracking, checkpoint decisions), producing I/O commands as buffered side effects — same pattern as `SwimActor` owning `Swim`. `SegmentSender` typed wrapper for mailbox communication.

## Produce Routing

DataPlaneActor resolves the target SegmentActor:

1. `hash(topic_name)` → shard group (metadata ownership)
2. Query MetadataStateMachine: `topic_name_index` → `TopicMeta` → `active_ranges` → find range whose keyspace contains `key`
3. Range's `active_segment` → `SegmentMeta.replica_set` → segment leader
4. If this node is segment leader: DataPlaneActor routes to local SegmentActor
5. If not: forward to segment leader's DataPlaneActor (1 hop max, same pattern as metadata propose forwarding)

## Producer Behavior on Seal

When a segment is sealed mid-stream (follower failure, size limit, or time limit), the SegmentActor has pipelined appends in flight. The broker responds with an error + redirect for all pending (un-ACKed) appends:

```
Sealed {
    stream_id,
    failed_ack_seqs: [seq1, seq2, ...],
    new_segment_id,
    new_leader: NodeAddress,
}
```

**Follower failure (common case):** Segment leader preserved at `replica_set[0]` — producer's TCP connection stays open. Producer retries un-ACKed records against `new_segment_id` over the same connection. Sub-millisecond overhead.

**Leader failure:** TCP connection drops (node is dead). Producer reconnects to any node, discovers new segment leader via metadata query (`GetShardInfo`), retries un-ACKed records. Same reconnection path as any TCP failure.

One retry code path in the producer handles both cases: receive `Sealed` or connection drop → retry un-ACKed records against the current segment leader (same node or rediscovered).

## Consume

Client → any replica in segment's `replica_set` → read from segment file.

Response includes:
- Records (batch of payloads)
- `has_more: bool` (more records available at higher offsets)
- `sealed: bool` (segment/range is sealed — no more writes)
- If sealed: `end_offset`, `split_into` or `merged_into` lineage pointers

### Follower Read Safety: Commit Offset

ALL-replica ack means a follower may briefly have records fsynced locally before the leader has confirmed commitment (waiting for other replicas' acks). To prevent consumers from reading uncommitted records, the leader piggybacks a `commit_offset` on each `ReplicaAppend`:

```
ReplicaAppend {
    ...
    records,
    commit_offset: u64,   ← highest offset committed as of this batch
}
```

Followers track `commit_offset` and only serve consumer reads up to it. Cost: one extra `u64` per replication message — negligible. The window between follower fsync and commit confirmation is typically sub-millisecond, so the read delay is invisible in practice.

This is NOT a Kafka-style high watermark — no ISR tracking, no watermark advancement protocol, no consumer-side watermark awareness. It's a single field piggybacked on replication traffic that the leader already sends. Consumers don't see it — they just get records up to the follower's known commit point.

## Metadata Query for Routing

DataPlaneActor needs read access to MetadataStateMachine state (topic → range → segment mapping) to route produce/consume requests. Two options:

- **Option A:** DataPlaneActor queries MultiRaftActor via channel (request-reply, like existing `GetLeader`)
- **Option B:** MetadataStateMachine state replicated to DataPlaneActor via applied-entry events

Option A is simpler initially. Option B may be needed later for performance (avoid per-produce round-trip to Raft actor).
