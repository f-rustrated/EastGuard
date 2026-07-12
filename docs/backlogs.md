# Stats

Not 1.0 yet.

The DS-RSM, storage, replication, sealing, rolling, repair, and recovery
foundations are substantial.
However, the producer and consumer-group layers contain correctness gaps
capable of misrouting records, losing committed offset progress, violating
advertised delivery semantics, or allowing concurrent range ownership.

I would call the current state a strong 0.x preview, not a stable 1.0 messaging
platform.

## Release blockers

### Critical — producer batches can cross a range split[Resolved]

Records are buffered under the range resolved when each call enters send()
(src/client/producer/mod.rs:65). At flush time, the entire opaque batch is
routed using only the first record’s key (src/client/producer/mod.rs:147).

The broker also chooses the destination range exclusively from that one routing
key and stores the whole opaque payload there (src/connections/
controller.rs:290).

Failure sequence:

1. Keys A and Z are buffered in the same parent range.
2. The parent splits between buffering and flushing.
3. The batch is routed using A.
4. Z is stored in A’s child range, although Z belongs to the other child.
5. A consumer following key-range lineage may never observe Z.

This is a data-routing correctness violation. Fix by re-resolving and
repartitioning every pending batch immediately before publication, or by making
the broker validate/reroute every record.

### High — offset commit failures are reported as success[Resolved]

write_offset_commits() logs serialization and network failures, skips the
failed range, and still returns Ok(()).

During revocation, the caller interprets that as success and removes its local
offset state (src/client/consumer/group.rs:329). Ownership can then transfer
while the durable offset remains stale.

Consequences include large duplicate replays, misleading successful commit()
results, and loss of the best-known progress during rebalance. Failed ranges
must remain pending and the aggregate commit must return an error.

### High — AtMostOnce does not provide at-most-once delivery [Resolved]

For AtMostOnce, the position is only marked committable before returning the
record (src/client/consumer/mod.rs:251). Durability still depends on a later
automatic or manual commit.

A process crash after next_record() returns but before the commit timer fires
causes that record to be delivered again after restart. That is not at-most-
once.

Resolved: Renamed the semantic to `AtMostOnceBestEffort` to align client expectations
with the performance reality of asynchronous commits, since strict at-most-once under
a record-by-record API requires an unacceptable 1-to-1 sync commit roundtrip per message.

### High — consumer groups lack ownership fencing

Each member independently derives ownership from its locally observed heartbeat
set (src/client/consumer/group.rs:388). There is no generation, lease epoch,
assignment record, or broker-side fencing token attached to fetches and
commits.

During heartbeat propagation delays, partitions, pauses, or asymmetric views,
two members can both believe they own the same range. Local owned_ranges checks
cannot prevent the other member from fetching or committing.

For 1.0, introduce a durable assignment generation or lease mechanism and
reject stale-generation commits. If the decentralized design is retained
without fencing, document consumer groups as best-effort and at-least-once
only.

### High — internal offset and assignment topics use replication factor 1 [Resolved]

Both system topics are automatically created with replication_factor: 1 (src/
client/consumer/group.rs:169).

A single broker loss can therefore remove consumer offset history or membership
history—the exact state needed during failover. These topics should use a
configurable, durable cluster-level replication factor, normally at least
three.

Resolved: Defaulted system topic replication factor to 3. On smaller clusters/tests,
this gracefully degrades to the number of available nodes via topology selection.

### High — producer retry duplication is not actually mitigated

Producer IDs and sequence numbers are allocated (src/client/producer/mod.rs:80)
but discarded during serialization. Neither value appears in ProduceRequest
(src/connections/protocol/data_plane.rs:30), and the server explicitly has no
idempotency handling (src/connections/controller.rs:64).

A response loss after a durable write causes a retry to create a second entry.
This is acceptable only if 1.0 clearly promises at-least-once production. Do
not describe the existing fields as functional idempotency.

### High — metadata logs grow without bounds

Snapshot metadata, snapshot data, applied index, and epoch keys remain reserved
and unused (src/impls/metadata_storage.rs:25). The roadmap confirms that
snapshot creation/install, restore, log compaction, and applied-index
persistence remain unimplemented (diagrams/metadata-management/
metadata_management_roadmap.md:151).

For a long-running 1.0 cluster this means:

- Unbounded metadata storage growth.
- Increasing restart/replay time.
- No practical way to bootstrap a far-behind replica after old logs eventually
need removal.

- Operational risk proportional to cluster age.

Snapshotting and compaction should be a 1.0 gate, or the release must carry
explicit workload and lifetime limits.

### High — resurrected nodes retain stale active-segment ownership

SWIM now permits a still-running node to refute a `Dead` observation at a
higher incarnation after a partition heals. The cluster may already have rolled
every active segment that named that node and committed successor replica sets,
but the resurrected process does not run crash recovery and may retain its old
in-memory active-segment assignments.

The restarted-node path does not close this gap: restart creates a fresh node
identity, discards active ownership, and treats recovered files as passive sealed
inventory. A live resurrection keeps the old identity and memory. It may therefore
continue accepting requests against a segment the metadata authority has sealed,
without receiving either the committed roll or an explicit revocation. Requiring
all data replicas to acknowledge prevents those stale writes from committing in
the usual partition case, but that is not an ownership fence and does not reclaim
the stale state.

Add an authoritative reconciliation path when a node resurrects:

- Fence writes until its active assignments have been checked against committed
  metadata.
- Revoke and locally seal assignments that no longer name the node.
- Preserve only data that can safely participate in sealed-segment catch-up;
  discard or quarantine private uncommitted tails.
- Make the reconciliation idempotent across repeated Alive gossip and another
  partition during recovery.

Cover this with a deterministic partition/heal test in which the old segment
leader is declared dead, a successor segment commits, and the same process later
refutes Dead. The test must prove that the old segment cannot accept a successful
write after resurrection and that the successor remains the sole active segment.

## Important pre-1.0 gaps

### Stale local Raft replicas are not retired after ring displacement

A peer removed by a committed Raft membership change is no longer a replication
target, so a partitioned copy cannot rely on receiving and applying the log entry
that evicted it. Applying peer removal also does not invoke group destruction;
that lifecycle is a separate explicit operation. After the partition heals, ring
reconciliation can stage the node as a learner for groups it should host again,
but groups it no longer owns can remain locally instantiated with obsolete logs
and timers.

Add local group-lifecycle reconciliation against the stable ring view. A group
that no longer belongs on the node should stop elections and traffic promptly,
then delete its persisted state only after a stability window protects against
ring flapping. A group assigned back to the node should reuse its durable prefix
only through learner catch-up, with the current leader remaining authoritative.
Exercise both outcomes under partition/heal simulation: re-admission to the same
group and permanent displacement from it.

### Offset restoration is expensive and can return a partial snapshot

Every rebalance reads the offsets topic from earliest and filters client-side
across all groups (src/client/mod.rs:157). After receiving the first record, a
100 ms quiet period terminates the scan (src/client/mod.rs:175).

Cold reads, parallel range scheduling, or transient latency can therefore
produce an incomplete offset map, causing unnecessary replay. It also becomes
O(total offset history) per rebalance.

Offsets need compacted-key semantics, a direct lookup API, or an authoritative
indexed state store.

### Unknown-end sealed-segment recovery [Resolved]

Unknown-end segments remain durable recovery candidates after the initial gather
budget expires. Periodic reconciliation and coordinator takeover restart boundary
discovery; a later complete gather corrects the boundary exactly once and ordinary
sealed-segment repair resumes.

Recovery remains automatic; there is no separate operator retry path to maintain.
Unexpected and conflicting reports are rejected without changing gather progress.

### No stable wire or storage-format compatibility policy

Wire enums use direct Borsh serialization without a protocol negotiation or
version envelope. Log-format versioning is still described only as a future
response to incompatible changes. A 1.0 promise needs:

- Protocol and storage format versions.
- Mixed-version compatibility rules.
- Rolling-upgrade tests.
- Unknown-field/variant behavior.
- Downgrade policy.
- Migration tooling or an explicit “no rolling upgrades” limitation.

### Public SDK lifecycle and API stability need work

The crate still declares version 0.1.0 (Cargo.toml:1), while the client module
globally suppresses dead and unused API warnings (src/client/mod.rs:1). There
is no explicit producer close contract, delivery callback model, structured
server errors, compatibility matrix, or semver/public-API check.

### Security is not production-ready

Client and inter-node protocols have no authentication, authorization, or
transport encryption. Any reachable client can create/delete topics and
produce/fetch data. At minimum, 1.0 documentation must define the trusted-
network boundary; for a general production release, TLS and authorization are
expected release gates.

## Validation and test-readiness

Completed successfully:

- cargo test --all-targets --all-features
    - 528 library tests passed.
    - 6 CLI tests passed.
    - 0 failures.
    - 1 ignored.

- cargo clippy --all-targets --all-features -- -D warnings
    - Passed.

The ignored test is material: the 100-seed simulation campaign has known
failures. Partition/heal faults have been implemented in the scenario runner
(src/it/sim/scenario.rs:305-322). The simulation backlog explicitly calls for
partition, targeted-leader, flapping, and nightly campaigns (src/it/sim/
README.md:100).

Before 1.0, add deterministic coverage for:

- Producer buffering across split and merge boundaries.
- ACK-loss retry duplication.
- Crash between at-most-once position update and delivery.
- Offset commit failure during revocation.
- Conflicting consumer-group membership views.
- System-topic replica loss.
- Network partitions during Raft membership transitions and segment rolls.
- Same-identity SWIM resurrection after Raft-peer eviction and active-segment
  replacement.
- Rolling upgrades with mixed protocol/storage versions.
- Long-running metadata growth and snapshot restore.

## Recommended release gates

1. [**Resolved**]Fix producer batch routing across splits.
2. Make offset commit failures observable[[**Resolved**]] and retryable.
3. [**Resolved** : AtMostOnceBestEffort] Either implement real at-most-once behavior or remove the claim. 
4. Add consumer ownership generations/fencing.
5. [**Resolved**] Replicate system topics durably.
6. Decide and document producer delivery semantics; ideally implement
    deduplication.

7. Implement metadata snapshots and log compaction.
8. [**Partially Resolved**: partition/heal faults enabled in runner] Enable and stabilize partition-based simulation campaigns
9. Define wire/storage compatibility and rolling-upgrade policy.
10. Establish security and operational support boundaries.

The sealing/rolling and replication work is close to being the strongest part
of the system. The remaining blockers are concentrated around client-visible
guarantees and long-term operability—precisely the areas a 1.0 label makes
difficult to revise later.
