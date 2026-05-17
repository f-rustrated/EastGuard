# Phase D2: Segment Replication

**Goal:** Primary-backup replication. fsync on ALL replicas before producer ACK. Seal-on-failure.

**Depends on:** Phase D1 (storage engine).

---

## Replication Protocol

```
Producer
   |
   v
Segment Leader
   |── batch records in memory
   |
   |── ┌── local WAL write + fsync ──────┐
   |── └── fan-out ReplicaAppend ─────────┘  in parallel
   |        (followers WAL fsync before ack)
   |
   |── wait for ALL: local fsync + all follower acks
   |── ACK to producer (records committed)
   |── insert into cache (in-memory, serves reads immediately)
   |
   v (background, off critical path)
   |── checkpoint: flush cache → segment file (buffered I/O + FADV_DONTNEED)
   |── update sparse index
```

Local WAL fsync and follower fan-out happen in parallel. Since `network_rtt + remote_fsync > local_fsync` in practice, the local fsync is hidden behind replication latency. Produce latency = `max(local_fsync, max(follower_fsyncs))`.

Followers use the same DataPlaneActor accumulation buffer as the leader. `ReplicaAppend` records are buffered by segment key and flushed when the global batch trigger fires. One WAL fsync covers all pending segments — the same mechanism as the leader role. `ReplicaAck` is sent to the leader after WAL fsync and cache publish. Background checkpoint to segment files follows the same path as the leader.

**Latency tradeoff:** The follower's accumulation window adds up to the batch trigger threshold to produce latency, because the leader waits for `ReplicaAck` before advancing `commit_offset` and ACKing the producer. Under high throughput the window is sub-millisecond; under low throughput it can reach the full 10ms batch interval.

### CommitNotify

After all `ReplicaAck`s are received, the segment leader advances its own `commit_offset` and sends `CommitNotify { segment_id, commit_offset }` to all followers **before** ACKing the producer:

```
all ReplicaAcks received
    → leader advances commit_offset
    → CommitNotify → followers
    → ACK producer
```

This gives a strict guarantee: *producer ACK ⟹ all followers already have the correct `commit_offset`*. Two problems are resolved by this ordering:

1. **Data loss on leader death.** If the leader dies after ACKing a producer, the coordinator can query surviving followers for their `commit_offset` and get the correct `end_offset` to propose in `RollSegment`. Without CommitNotify, followers would show a stale `commit_offset` (1-batch lag from the piggybacked field), and the coordinator would seal at a lower offset — losing ACKed records.

2. **Stuck consumers on idle topics.** Without CommitNotify, a follower's `commit_offset` only advances when the next `ReplicaAppend` arrives. On an idle topic, a tailing consumer on a follower waits indefinitely even though the data is committed and on disk. CommitNotify ensures `commit_offset` is current before the topic goes idle.

**CommitNotify is fire-and-forget — no reply expected.** The leader sends `CommitNotify` and immediately ACKs the producer without waiting for any response from followers. This is safe because `CommitNotify` is not a durability operation — durability was already established in the `ReplicaAck` round. `CommitNotify` is purely a visibility update. TCP guarantees delivery; no application-level ACK is needed. The guarantee that matters is not "followers processed CommitNotify before producer ACK" but "followers processed CommitNotify before the coordinator can act on leader death" — SWIM detection takes ~6–7 seconds, giving TCP more than enough time to deliver a ~100-byte message.

**Why not follower polling?** An alternative is for followers to poll the leader via `QueryCommitOffset` after a timeout. This resolves the stuck consumer problem but not the data loss problem — there is no causal ordering between the producer ACK and a follower's poll, so if the leader dies between ACKing the producer and the poll completing, the follower still shows a stale `commit_offset`. CommitNotify (push, before ACK) is required for the data loss guarantee.

## Replica Authorization on ReplicaAppend

When a segment leader starts replicating a new segment (e.g., after seal-and-replace), followers may not know about the new segment yet. The first `ReplicaAppend` for a new segment carries the segment metadata — followers validate and act without waiting for any notification:

```
ReplicaAppend {
    shard_group_id,
    range_id,
    segment_id,
    replica_set: [D, E, G],    // from SealResponse / metadata
    records,
    commit_offset: u64,        // highest committed offset (see D6 "Follower Read Safety")
}
```

Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create segment file, write, ack

The replication message is self-authorizing — no gossip or coordinator notification required for followers on the data path. Nodes not in the replica_set learn via metadata query if needed.

No quorum. No ISR. ALL replicas must ack. If any fails → seal segment, open new one.

## Failure Response

### Follower Failure (detected by segment leader)

When a follower fails to ack within timeout:

1. Segment leader (broker) stops accepting new produces for this segment
2. Segment leader sends `SealRequest` to coordinator (vnode leader), including `end_offset` — the last offset committed (ACKed by all replicas before the failure)
3. Coordinator proposes `RollSegment` via Raft with `new_replica_set` excluding failed node and `end_offset` from the SealRequest. Previous segment leader preserved at `replica_set[0]` (cache locality, active producer connections).
4. Raft commits → MetadataStateMachine seals old segment (sets `end_offset`), creates new segment (`start_offset = end_offset + 1`)
5. Coordinator notifies affected brokers via `data_port`
6. Segment leader opens new segment file with healthy replica set
7. Blocked producer streams resume against new segment — un-ACKed records retried by producer against new segment
8. Sealed old segment queued for under-replication repair (async)

### Leader Failure (detected by SWIM)

Leader failure is detected via SWIM node death (~6-7s). When the coordinator receives `HandleNodeDeath` for the segment leader, it proposes `RollSegment` for all affected active segments — same mechanism as any SWIM-triggered seal.

1. SWIM detects leader (D) as dead → coordinator receives `HandleNodeDeath(D)`
2. Coordinator sends `QueryCommitOffset { segment_id }` to all surviving followers of affected segments. Takes `max(commit_offset)` from responses as `end_offset`. This is safe because CommitNotify guarantees *producer ACK ⟹ followers already have the correct `commit_offset`* — querying survivors after leader death always yields the true sealed boundary.
3. Coordinator proposes `RollSegment { end_offset }` via Raft with `new_replica_set` excluding D. A surviving follower is promoted to `replica_set[0]` (new segment leader).
4. Raft commits → MetadataStateMachine seals old segment, creates new segment
5. Coordinator sends `SegmentAssignment` to new segment leader via `data_port`
6. New segment leader accepts produce. Producer discovers new leader via metadata query.
7. Sealed old segment queued for under-replication repair (async)

**Open questions:**
- If no follower responds to `QueryCommitOffset` (all followers also dead), what is the fallback `end_offset`?
- What timeout before falling back?

Follower failure uses `SealRequest` (segment leader → coordinator). Leader failure uses the SWIM `HandleNodeDeath` path (coordinator-initiated, no `SealRequest` needed).

**RollSegment idempotency:** Both paths may fire for the same failure — write-path timeout and SWIM detection can race. MetadataStateMachine's `apply_roll_segment()` checks preconditions — if the segment is already sealed, subsequent proposals are no-ops (DS-RSM invariant 9).

### Offset Handoff: Data Plane → Metadata Plane

MetadataStateMachine does not see individual records (proposing per-produce would be prohibitively expensive). The segment leader (SegmentActor) is the authoritative offset tracker during normal operation. On seal, the offset is handed off via `SealRequest`:

```
SealRequest {
    shard_group_id,
    range_id,
    segment_id,
    failed_node: NodeId,
    end_offset: u64,       ← last committed offset, from SegmentActor
}
```

The vnode leader carries `end_offset` through to the `RollSegment` command. `apply_roll_segment()` uses it (instead of `range.next_offset`) to seal the segment and derive the new segment's `start_offset`. This maintains invariant 8 (offset continuity) while keeping per-record offset tracking in the data plane.

### Orphaned Records Past end_offset

The leader's WAL (and healthy followers' WALs) may contain records past `end_offset` — fsynced locally but never committed because the failed follower never ACKed. These records are NOT part of the sealed segment. Consumers MUST respect `end_offset` and never read past it, even if the segment file has more bytes. WAL rotation eventually deletes the files containing orphaned records.

## Transport

`DataTransportActor` — TCP listener on `data_port`. Persistent bidirectional connections between nodes (same pattern as `RaftTransportActor` — `HashMap<NodeId, OwnedWriteHalf>`, lower-NodeId-wins conflict resolution, length-prefixed bincode frames).

Separate from Raft TCP transport (`raft_port`) because:
- Different traffic patterns (high throughput data vs low throughput consensus)
- Independent backpressure (slow data replication should not delay Raft heartbeats)
- Can be tuned independently (buffer sizes, TCP_NODELAY, SO_SNDBUF)

## Data Port Message Catalog

All `data_port` wire messages, consolidated across phases:

| Message | Direction | Purpose | Phase |
|---|---|---|---|
| `ReplicaAppend` | Segment leader → followers | Replicate records + piggybacked `commit_offset` | D2 |
| `ReplicaAck` | Follower → segment leader | Confirm WAL fsync for a batch | D2 |
| `CommitNotify` | Segment leader → followers | Advance follower `commit_offset` before producer ACK; sent when no next `ReplicaAppend` is imminent | D2 |
| `QueryCommitOffset` | Coordinator → followers | Query `commit_offset` after leader death, before proposing `RollSegment` | D2 |
| `CommitOffsetResponse` | Follower → coordinator | Reply to `QueryCommitOffset` | D2 |
| `SealRequest` | Segment leader → coordinator | Request segment seal (carries `end_offset`) | D2 |
| `SealResponse` | Coordinator → segment leader | New segment ID + replica set | D2 |
| `SegmentSealed` | Segment leader → old followers | Notify seal, stop accepting writes | D2/D3 |
| `SegmentAssignment` | Coordinator → new segment leader | Assign new/rolled segment to leader | D3 |
| `CatchUpRequest` | Replacement node → healthy replica | Request sealed segment data (carries `local_end_offset`) | D2 |
| `CatchUpResponse` | Healthy replica → replacement node | Stream segment data (delta from `local_end_offset`) | D2 |
| `Sealed` | Broker → producer | Error + redirect on seal (carries `new_segment_id`, `new_leader`) | D6 |

## Replication vs Consensus

| Property | Raft (metadata) | Segment replication (data) |
|---|---|---|
| Model | Consensus (leader election, log ordering) | Primary-backup (leader-driven, seal-on-failure) |
| Ack | Majority quorum | ALL replicas fsync |
| Failure handling | Raft re-election, log reconciliation | Seal segment, open new one. No reconciliation. |
| Ordering | Total order (log index) | Per-segment order (offset) |
| Transport | raft_port (TCP) | data_port (TCP) |

## Sealed Segment Replication (self-healing)

When a sealed segment is under-replicated (replica count < `replication_factor`), the coordinator assigns a replacement node, updates the sealed segment's `replica_set` via Raft (`ReassignSegment`), and triggers repair. The replacement sends `CatchUpRequest` to a healthy replica — the only use of `CatchUpRequest` in the system. In the seal-on-failure model, active segments never need catch-up: any replica failure triggers seal-and-replace, and the new segment starts fresh with all replicas in sync from `start_offset`.

Nodes get new NodeIds on restart (UUID regenerated) — from the cluster's perspective, a recovered node is a new member. However, its local disk may still contain segment data from a previous lifecycle. On `CatchUpRequest`, the replacement node checks its local segment inventory and advertises its `local_end_offset` — the healthy replica streams only the delta, or nothing if the data is already complete. See roadmap "Local Data Reuse on Recovery" for details.

---

## Example Scenario: Seal-on-Failure with Separate Metadata and Data Nodes

```
Metadata group (Raft): vnodes [A, B, C]    ← manage topic "orders" metadata
Data replicas:         brokers [D, E, F]    ← store segment 7 of range 0

Topic "orders" created via:
  Client → ProposeRequest(CreateTopic) → vnode leader A
  A proposes via Raft, commits on [A, B, C]
  Coordinator assigns replica_set = [D, E, F] (based on storage policy)
  A sends SegmentAssignment to D via data_port TCP     ← direct notification
  D creates segment 7 file, ready to accept produce
  D = replica_set[0] = segment leader
```

**Step 1: Normal produce flow**
```
Producer ──> Broker D (segment leader)
               |── WAL append + fsync
               |── ReplicaAppend ──> E (WAL fsync, ack) ✓
               |── ReplicaAppend ──> F (WAL fsync, ack) ✓
               |── ACK producer
               |── async: segment file + index
```

**Step 2: F fails (disk failure, crash, partition)**
```
Producer ──> Broker D
               |── WAL append + fsync
               |── ReplicaAppend ──> E ✓ ack
               |── ReplicaAppend ──> F ✗ timeout
               |
               D detects: F failed for segment 7
```

**Step 3: D requests seal from metadata coordinator (RPC)**
```
D ──(SealRequest RPC)──> vnode A (Raft leader for shard group #45)
                                |
                                A proposes RollSegment {
                                  topic: "orders",
                                  range: 0,
                                  old_segment: 7,
                                  new_replica_set: [D, E, G]  ← F replaced with G
                                }
                                |
                                Raft commits on [A, B, C]
                                MetadataStateMachine: segment 7 = Sealed
                                                      segment 8 = Active, replica_set = [D, E, G]
                                |
D <──(SealResponse)────────────|
      { new_segment_id: 8, new_replica_set: [D, E, G] }
```

Broker D learns the result immediately via RPC response (~100ms). No gossip wait.

**Step 4: D resumes, notifies participants via `data_port`**
```
Broker D: opens segment 8 file, resumes produce on segment 8 with [D, E, G].
          (~100ms total downtime)

D notifies via data_port:

Broker E: receives SegmentSealed from D. Closes segment 7 for writes.
           Segment 7 data on E is complete (was caught up before seal).

Broker G: receives first ReplicaAppend for segment 8 from D (self-authorizing).
           Validates replica_set, opens segment 8 file, starts accepting.

Broker F: (if recovered, with new NodeId) is a new cluster member.
           F is NOT in segment 8's replica_set.
           If later assigned to a sealed segment's replica_set, F checks
           its local disk for existing data and advertises local_end_offset
           in CatchUpRequest — skipping data it already has.
```

**Step 5: Sealed segment repair (async)**
```
Coordinator detects: segment 7 under-replicated (F's copy incomplete)
Assigns new replica: broker H

Broker E ──(consume protocol)──> Broker H
  Read segment 7 records → write to H's segment 7 file
  H now has complete copy. Segment 7 fully replicated.
```

### Safety Between Seal and Notification

Between Raft commit (step 3) and `data_port` notifications reaching participants (step 4), there's a brief window (~milliseconds over TCP, not seconds via gossip). This is safe:

- **Segment leader D** already stopped writing to segment 7 (D initiated the seal). No new data enters segment 7.
- **Follower E** learns immediately via `SegmentSealed` message from D over `data_port`. Not waiting for gossip.
- **New replica G** learns from D's first `ReplicaAppend` for segment 8 (self-authorizing). No waiting.
- **Failed node F** is unreachable. When it recovers, it queries the coordinator or gets rejected on the next interaction. F never writes stale data.
- **No legitimate produce traffic** targets segment 7. Producers connect to segment leader of the **active** segment (segment 8 on D).
- **Consumer reads** of segment 7 are fine — sealed segments are immutable and readable from any replica that has the data.
