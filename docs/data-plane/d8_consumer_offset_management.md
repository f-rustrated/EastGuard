# D8: Consumer Offset Management

**Goal:** Enable consumers to durably record their progress (offsets) and collaboratively distribute work across ranges (Consumer Groups) without introducing a massive, complex server-side state machine.
**Depends on:** [D6: Produce/Consume API](d6_produce_consume_api.md)

---

## The Landscape: How Others Do It

Building a robust consumer group mechanism is historically one of the most complex parts of a streaming broker. Before defining EastGuard's approach, we evaluate the industry standards:

1. **Kafka (Heavy Server Coordinator):**
   - Uses an internal `__consumer_offsets` topic for storage.
   - A server-side "Group Coordinator" manages complex JoinGroup/SyncGroup/Heartbeat state machines.
   - *Tradeoff:* Extremely robust, but building the coordinator state machine is a massive engineering investment (tens of thousands of lines of code).

2. **Pulsar (Broker-managed Subscriptions):**
   - The broker actively serving a partition also manages the consumer cursors in-memory, writing them durably to BookKeeper.
   - *Tradeoff:* Couples data routing with consumer state. In EastGuard, ranges split/merge and segments move dynamically, making it difficult to pin cursor state to a specific data node.

3. **NATS JetStream (Server-side Tracking):**
   - The server entirely owns the consumer state, tracking per-message ACKs or sequence cursors via Raft.
   - *Tradeoff:* High operational overhead on the consensus layer if not carefully sharded.

---

## The EastGuard Approach: Decoupled & Resource-Efficient

EastGuard draws architectural inspiration from systems like LinkedIn's Northguard, cleanly separating the control plane (metadata) from the data plane (payloads). To achieve horizontal scalability far beyond what centralized coordinators can manage, EastGuard rejects the monolithic Kafka-style Group Coordinator entirely.

Instead, we solve Consumer Groups by combining our two existing, battle-tested primitives:

1. **High-throughput offset storage:** Use the Data Plane (an internal topic `__eastguard_offsets`).
2. **Client-side work assignment:** Use the Data Plane for peer discovery (`__eastguard_assignments`).

This completely eliminates the need for a dedicated "Coordinator" subsystem.

---

## 1. Offset Storage: The Internal Topic

Offsets change thousands of times per second. They belong in the data plane.

We introduce a system topic: `__eastguard_offsets`.
- **Producing (Frequency):** The client acts strictly as a Producer to this topic during normal operation. The frequency of commits is controlled by the client SDK:
  - *Auto-commit:* The client automatically publishes the highest processed offset in the background (e.g., every 5000ms).
  - *Manual commit:* The application developer disables auto-commit and explicitly calls `consumer.commit()` after processing a batch.
- **Consuming (On Range Assignment):** The client does *not* continuously subscribe to this topic. It performs a one-off read **only at the exact moment** it takes ownership of a range (either during application startup or when taking over a crashed peer's range) to find its starting position.
  - *Offset Found:* If a saved `next_entry_id` exists, the consumer resumes from that exact entry, ignoring the user's configured `StartPolicy`.
  - *No Offset Found:* If this is a brand new consumer group, it falls back to the user's `StartPolicy` (e.g., `Earliest` starts at 0, `Latest` starts at the current end of the log and waits for new messages).
- **Compaction:** Because offsets represent a continuous state overwrite, the `__eastguard_offsets` topic will eventually utilize log compaction (retaining only the latest payload per key) to bound its size.

*Why this works:* We get replication, durability, and high throughput for offset commits entirely for free by reusing the D6 Produce/Consume API.

---

## 2. Work Distribution: Client-Side Cooperative Assignment (Zero-Controller)

Using the Control Plane (`MetadataStateMachine` / Raft) to manage ephemeral consumer leases or heartbeats is fundamentally incompatible with EastGuard's design:
- It pollutes structural metadata with ephemeral client state.
- It injects wall-clock time requirements into a purely deterministic state machine.
- High-volume heartbeats would crush the consensus layer, destroying performance.

Instead, we push work distribution entirely to the client side, using the Data Plane as our communication channel. 

We introduce a second system topic: `__eastguard_assignments`.
- **Heartbeats & Discovery:** The client runs a background publisher that periodically produces a heartbeat payload (e.g., `[group_id, consumer_id] -> timestamp`) to this topic. Crucially, the client also holds a **continuous background receiver** on this topic to constantly monitor the health of all its peers.
- **Deterministic Assignment:** Once a consumer knows the current active membership of its group, it deterministically hashes and divides the available ranges (discovered via standard metadata `DescribeTopic` calls) among the live members. 
- **Conflict Resolution (Zero-Controller):** Because all consumers consume the same `__eastguard_assignments` log, they eventually reach a consistent view of the group membership. There is no server-side "Coordinator" tracking generations or leases.
- **Failover (Timeout):** If a consumer (e.g., Consumer B) crashes, it stops publishing heartbeats. Because the surviving peers (like Consumer A) are actively reading the assignment log, they will notice that B's heartbeat is missing for a defined threshold (e.g., 10 seconds). The peers deterministically remove B from their active lists, recalculate the hash assignment, and seamlessly take over B's orphaned ranges.

*Why this works:* It shifts the complexity of work-balancing entirely to the client edge and keeps the server completely agnostic to consumer groups. The Control Plane remains strictly focused on cluster topology and range structural metadata.

---

## 3. System Topic Keyspace Routing & Scaling

Because EastGuard brokers treat `__eastguard_offsets` and `__eastguard_assignments` as completely ordinary topics, they are fully compatible with EastGuard's dynamic **keyspace routing** and **auto-splitting**.

Unlike older systems that route internal topics using a fixed partition index (`hash(group_id) % N`), EastGuard clients route by mapping their group to a point in the `u64` keyspace:

1. **The Routing Key:** 
   - For `__eastguard_assignments`, the routing key is the `group_id`.
   - For `__eastguard_offsets`, the routing key is `[group_id, topic_id]`.
2. **Deterministic Hashing:** The client SDK hashes this key into a deterministic `u64` keyspace value.
3. **Range Discovery:** The client issues a `DescribeTopic` request for the system topic. It inspects the active ranges and finds the specific range whose `[keyspace_start, keyspace_end]` boundaries contain its `u64` hash. It then routes its heartbeats/commits to that range.
4. **Transparent Auto-Splitting:** If a system topic range gets too hot, the EastGuard data plane dynamically splits it. The keyspace is divided, but the client's `u64` hash remains the same. Upon the next metadata refresh, the client naturally discovers the new child range that inherited its portion of the keyspace, seamlessly transitioning its traffic without any central coordination.

---

## 4. Transition Handling (Splits and Merges)

Because EastGuard dynamically splits and merges ranges, consumer groups must navigate lineage cleanly.

When a consumer is assigned to a range that becomes sealed (e.g., a Split):
1. The consumer drains the final segment and discovers the in-band transition signal (as defined in D4).
2. The consumer commits its final offset to the `__eastguard_offsets` topic.
3. The consumer publishes a "Range Sealed" metadata event to the `__eastguard_assignments` heartbeat topic.
4. The consumer group observes the split/seal event from the assignment topic, issues a fresh `DescribeTopic` to the Control Plane to discover the new child ranges, and re-runs the deterministic hash to assign the new ranges across the group.

If the consumer crashes during the transition, another consumer in the group will eventually take over the child ranges during the next deterministic rebalance round.

---


## 4. Architectural Rules

1. **Eventual Consistency in Assignment.**
   *Why:* Because assignment relies on a data plane heartbeat topic, short-lived duplicate assignments (two consumers reading the same range) may occur during network partitions before they converge on a consistent view. Consumer applications must tolerate this (e.g., via idempotent processing).
2. **Offset commits are completely unverified by the storage engine.**
   *Why:* The data plane does not parse or validate the contents of the `__eastguard_offsets` topic. It treats commits as opaque bytes. This maintains the strict boundary where the data plane is unaware of consumer logic.
3. **The Control Plane is completely oblivious to consumer state.**
   *Why:* `MetadataStateMachine` only tracks ranges and physical placement. Ephemeral group membership, heartbeats, and offsets belong strictly in the data plane.
