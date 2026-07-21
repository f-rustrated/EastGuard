# D8: Consumer Offset Management

**Goal:** Persist consumer-group progress on the data plane with the same durability and
placement model as range data, while using control-plane generations to prevent a previous
owner from advancing a range after reassignment.

**Depends on:** [D2: Segment Replication](d2_segment_replication.md),
[D4: Consumer Range Tracking](d4_consumer_range_tracking.md), and
[D6: Produce/Consume API](d6_produce_consume_api.md).

---

## Responsibility Split

Consumer groups cross three layers, but each layer owns a different kind of state:

| Layer | Responsibility | Why it belongs there |
|---|---|---|
| Control plane | Membership, active-range assignment, monotonic generation | Assignment changes are ordered metadata decisions and need one authoritative fence |
| Client | Delivery, acknowledgement, effective lineage ownership, auto/manual commit timing | Only the application side knows when processing is complete |
| Data plane | Durable offset value, generation validation, replication, recovery | Offset commits are frequent writes and should not enter the Raft log |

This is neither a hidden internal topic nor a client-only lease protocol. Group membership is
committed through the topic's metadata shard. Offset values are stored directly by the data
replicas that own the corresponding range.

```
                 control plane (Raft)
                membership + assignment
                         │
                  generation seal
                         ▼
client ── offset commit ──► range data leader ──► followers
  │                              │                    │
  └── assignment heartbeat       └──── shared WAL ────┘
```

The split keeps high-frequency commits off Raft without giving up fencing. Raft decides who
may write; the data plane makes those writes durable.

---

## Offset Identity and Position

A durable offset is keyed by:

```
(topic, range, consumer group)
```

The value identifies the last processed record using three coordinates:

| Coordinate | Purpose |
|---|---|
| Entry id | Locates the physical batch in the range log |
| Batch offset | Locates the record within that batch |
| Absolute offset | Preserves the record-level resume position exposed by the client |

Offsets are range-scoped. A segment roll keeps the same range and therefore the same offset
key. A split or merge creates new ranges with new offset keys; lineage handling decides when
the predecessor is drained and when successors may start.

The stored value advances monotonically by the structural coordinate. A duplicate or older
commit cannot move progress backward.

---

## Group Generations

Every membership or active-range topology change recomputes the complete assignment and
advances the consumer-group generation. A heartbeat that changes neither leaves the
generation unchanged.

When a new generation commits, the control plane publishes that generation to the data
replicas for every range in the topic, including the last placement of a sealed range. The
data plane durably records this **epoch seal** before accepting commits at that generation.

For an incoming commit:

| Commit generation | Data-plane action |
|---|---|
| Older than sealed generation | Reject as stale |
| Equal to sealed generation | Write and replicate |
| Newer than sealed generation | Park until the matching epoch seal arrives |

Parking handles normal propagation reordering: a client can learn the committed assignment
before the data replicas receive its seal. Accepting immediately would bypass fencing;
rejecting permanently would turn harmless propagation delay into an application failure.

---

## Leader-Coordinated Commit Path

Offset writes use the data replica set of the range, not the Raft replica set. The first data
replica is the offset coordinator, matching the range's data-write leader.

```
client                 range leader                 followers
  │                         │                           │
  │── commit(key, epoch) ──►│                           │
  │                         │ validate leadership      │
  │                         │ validate generation      │
  │                         │ append shared WAL        │
  │                         │ fsync                     │
  │                         │── replicate commit ─────►│
  │                         │                           │ append shared WAL
  │                         │                           │ fsync
  │                         │◄──── durable ack ─────────│
  │◄──── committed ─────────│ after every follower ack │
```

The client sends one request to the leader. It does not fan out commits itself. A request that
lands on a follower receives a write-leader redirect; the ordinary client retry path follows
that redirect.

The leader acknowledges only after its own WAL fsync and every follower's durable
acknowledgement. With a single replica, local fsync is sufficient. This matches the durability
contract of produced data and prevents an acknowledged offset from disappearing during
leader failover.

Reads are leader-authoritative as well. Reading a random replica could return a value behind
the acknowledged commit while replication or commit notification is in flight.

---

## Shared WAL, Separate State

Consumer offsets use the existing data-plane WAL rather than a second write path. Segment
records, epoch seals, and offset commits can share one batch and one fsync.

After fsync, the record type determines which in-memory state is updated:

- Segment records publish into the segment cache.
- Epoch seals advance the accepted generation for one offset key.
- Offset commits update the consumer-offset ledger.

This preserves sequential disk writes. A logically separate offset ledger does not imply a
second synchronous file write.

Follower acknowledgements are emitted only after the follower's shared WAL fsync. Applying a
record to the in-memory ledger before fsync is not a durability acknowledgement.

---

## Snapshotting and WAL Reclamation

The in-memory ledger is periodically materialized as one snapshot, but not once per commit.
The WAL remains authoritative until the snapshot covers the corresponding log positions.

```
offset commit fsync
      │
      ├── update in-memory ledger
      └── remember uncheckpointed WAL position
                         │
                  WAL file becomes reclaimable
                         │
                         ▼
             clone ledger for checkpoint worker
                         │
                  write + fsync snapshot
                         │
                         ▼
             advance offset checkpoint watermark
                         │
                  reclaim older WAL files
```

Snapshot creation is driven by reclamation pressure. Many commits therefore collapse into a
single asynchronous snapshot. WAL deletion uses the minimum safe watermark across segment
checkpoints and the auxiliary-state checkpoint, which contains only consumer offsets today;
neither state family can delete recovery data still needed by the other.

On restart, recovery loads the latest snapshot and replays later offset records from the
shared WAL. Recovery then writes the consolidated snapshot before removing the replayed WAL.

### Consolidation with producer frontier state

The implemented snapshot currently contains consumer-offset state only. D10 adds another
range-scoped, end-state ledger for producer retry frontiers. It must extend this checkpoint
into one **range-auxiliary-state snapshot**, rather than introducing a second independently
fsynced snapshot:

```text
shared WAL
  ├── consumer offset records
  └── produced entries carrying producer sequence identity
              │ reclamation reaches uncovered state
              ▼
one auxiliary-state snapshot at one LSN boundary
  ├── current consumer offsets + placement readiness
  └── current producer frontiers + bounded recent results
              │ durable rename completes
              ▼
advance one auxiliary checkpoint watermark
```

This follows the metadata log-to-snapshot model: replayable log records are authoritative
until one complete current-state image safely replaces their covered prefix. The snapshot
is asynchronous and reclamation-driven, so its single fsync is not part of offset-commit or
produce latency. Segment payload checkpointing remains separate because segment files retain
application data, whereas both auxiliary ledgers retain only their latest state.

### Migration from the consumer-only checkpoint

The D10 implementation should replace, rather than wrap, the current consumer-only snapshot
pipeline:

1. Replace the consumer-offset-only snapshot image with the typed auxiliary-state image.
2. Replace its dedicated checkpoint job, completion signal, in-flight flag, and watermark
   with one auxiliary-state checkpoint lifecycle.
3. Keep offset commits and graduation imports in the shared WAL; their WAL fsync is still the
   acknowledgement durability boundary.
4. On upgrade, load the legacy consumer-offset snapshot when no consolidated snapshot exists,
   combine it with producer frontier state recovered from WAL, and publish a durable
   consolidated snapshot before reclaiming either the legacy file or covered WAL.
5. Remove the legacy snapshot only after the consolidated snapshot's durable rename and
   directory sync complete.

This cleanup removes the independent consumer-snapshot fsync once D10 adds producer state. It
does not remove the shared-WAL fsync required before acknowledging an offset commit, nor the
single asynchronous fsync that makes a replacement snapshot safe for WAL truncation.

---

## Range Lifecycle and Placement

Offset coordination is range-scoped, while live data actors are segment-scoped. That
difference matters at transitions.

### Segment roll

A roll creates a successor segment in the same range. Offset identity does not change, and
the retained range placement follows the successor replica set.

### Split or merge

A sealed predecessor can remain effectively owned until it is drained, even though its live
segment actor has retired. The data plane therefore retains the range's last known placement
independently of the live segment actor. This allows the predecessor's final offset to be
committed and read after the transition.

The client expands active assignments through lineage so exactly one successor owner drains
each sealed predecessor. Once effective ownership drops the predecessor, its local tracker is
discarded and a stale generation cannot commit it.

---

## Rebalance and Stale-Commit Recovery

A normal rebalance is incremental:

1. Request the current assignment and generation.
2. Refresh metadata only if the cache cannot describe an assigned range.
3. Expand active assignment into effective lineage ownership.
4. Stop revoked range actors.
5. Load durable offsets at the assignment generation and start newly owned or previously
   unprovisioned ranges.

Unchanged actors continue running. Rebuilding every actor on each heartbeat would cause
avoidable duplicate delivery and control-plane traffic.

The assignment generation is also the distributed offset-read boundary. Range leaders answer
only after that generation's epoch seal is durable locally. Although different ranges may be
served by different leaders, every answer therefore includes all commits before the ownership
transition and excludes commits by the new owner. The recovering member serializes its own
commits with this lookup, so it cannot advance one requested range while the remaining range
leaders are still answering. This gives the range set one logical snapshot without a
cross-range transaction.

A stale commit is a stronger signal. The client fences delivery, stops all range actors,
rebuilds effective ownership from the latest generation, retries only positions still owned,
and provisions those actors again from durable offsets. The fence rejects late application
acknowledgements while the asynchronous actor stops are in flight.

---

## Failure Handling

| Failure | Behavior |
|---|---|
| Client routes to follower | Redirect to current data leader |
| Assignment arrives before epoch seal | Commit parks until the seal arrives |
| Offset read arrives before epoch seal is durable | Read is rejected; rebalance retries |
| Old owner commits after reassignment | Rejected by generation fence |
| Leader WAL fsync fails | Commit fails; no replication acknowledgement |
| Follower WAL fsync fails or connection disappears | Leader does not acknowledge the commit |
| Offset snapshot fails | WAL remains pinned and recovery stays possible |
| Durable-offset read is unavailable | New range actor remains unstarted; the client receives an error and a later rebalance retries |
| Broker restarts | Snapshot plus shared-WAL replay reconstructs the ledger |

---

## Rules

1. **Only the current generation may change an offset.** This prevents a previous owner from
   advancing progress after reassignment.
2. **The range data leader coordinates offset replication.** The client sends one logical
   commit; replication topology remains a server responsibility.
3. **Acknowledgement means every data replica is durable.** Returning earlier could lose an
   acknowledged checkpoint during failover.
4. **Offset records share the data WAL.** Separate synchronous write paths would defeat WAL
   batching and add another ordering boundary.
5. **Snapshots are reclamation-driven.** Per-commit snapshots would turn a batched WAL path
   into random-write amplification.
6. **Sealed predecessors retain offset placement until their final progress is durable.** A
   segment actor's retirement must not orphan the range-level checkpoint.
7. **Unavailable durable offsets fail closed.** Absence of a response is not evidence that a
   group has no saved progress.

See [C4: Consumer Offset Commits](../clients/c4_consumer_offset_commits.md) for the
application acknowledgement contract and
[D4: Consumer Range Tracking](d4_consumer_range_tracking.md) for lineage ordering.
