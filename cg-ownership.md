
## Problem Context
Consumer-group members currently derive range ownership from eventually consistent heartbeat observations. During partitions, delayed heartbeats, process pauses, or asymmetric network views, members can calculate different rings and simultaneously believe they own the same range, leading to split-brain data processing and offset regression.


Given EastGuard's decoupled architecture, the state management is strictly split:
* **The Metadata Layer (ML)** uses Raft consensus to manage and persist cluster metadata, topic configurations, and consumer group assignments (ownership).
* **The Data Layer (DL)** uses a separate, high-throughput I/O mechanism to manage and persist message data and consumer offsets.

The fencing solution must bridge this separation without forcing data commits to proxy through the ML, maintaining direct 1-hop Client-to-DL communication for high-throughput I/O.


## Architectural Direction

The fencing mechanism will utilize the distinct persistence layers of EastGuard, using a Generation ID (GID) as the security bridge between ML ownership and DL offset management.

### 1. Metadata Layer (Ownership & Raft Persistence)
* The ML acts as the sharded authority for consumer group membership. 
* It calculates assignments and uses Raft to strictly serialize and persist who owns what range.
* **Any** consumer group change (join, leave, crash) pauses the group and produces a strictly increasing **Generation ID (GID)**, which is safely committed to the ML's Raft log.


### 2. Data Layer (Offsets & Localized Persistence)
* **No Global Topic:** Offset commits are not sent to the ML or a global system topic. The ML provisions localized system ledgers (metadata segments) on the exact same DL nodes that host the user data ranges.
* The DL natively persists client offset commits directly to these localized ranges using its high-throughput engine.

### 3. The Bridge: Epoch Sealing & The Future Queue
To protect the DL's offset ledgers from stale clients, the ML must proactively push an changes in membership to the Data Layer whenever ownership changes.
* When the ML's Raft consensus commits a new assignment and GID, the ML broadcasts this new GID directly to the specific DL nodes hosting the affected ranges.

#### Race condition - Consumer 1 commits with GID 1 before the seal arrives
This is _fine_. If Consumer 1 sneaks in one final commit with GID 1 just milliseconds before the DL receives the GID 2 seal, the DL accepts it. This just means Consumer 1 successfully processed one last batch of data before being evicted. Because Consumer 2 is currently blocked (as we will see in Condition 2), Consumer 2 hasn't started fetching or committing yet. Therefore, there is no "split-brain" data interleaving. The handover remains clean, just slightly delayed.


#### Race condition - Consumer 2 arrives with GID 2 before the seal arrives
If the DL receives a request where Request GID > Local Sealed GID, the DL is looking at a message from the future. If the DL simply returned an InvalidEpochException, Consumer 2 would panic, go back to the ML, get GID 2 again, and hit the DL again—creating a vicious, resource-wasting retry loop until the seal finally arrives. 



### 4. Data Layer (The Monotonic Enforcer)
The DL enforces the fence locally. It does not know *who* owns the partition (that is the ML's job); it only knows the *current valid GID*.
* If `Request GID < Local GID`: **Reject** (`StaleEpochException` - Client is a zombie).
* If `Request GID == Local GID`: **Process** and append the offset to the DL's local system range.
* If `Request GID > Local GID`: **Queue** (Wait for ML seal).



### 5. The Fencing Loop (Client Recovery)
* When a zombie client wakes up and hits the DL's fence, the Client SDK catches the `StaleEpochException`.
* The Client immediately halts data processing and reconnects to the ML to request the latest Raft-backed assignment state and its new GID.


## Acceptance Criteria
* The ML strictly persists consumer group assignments and GIDs via Raft.
* The DL strictly persists offset commits via Co-located System Ranges, isolated from the ML.
* The ML successfully pushes Epoch Seals to the relevant DL nodes immediately after a Raft commit.
* The DL rejects offset commits with stale GIDs, protecting the integrity of the offset ledgers.
* The DL successfully suspends and unparks future-epoch requests without dropping them.
* Direct I/O is preserved: Offset commits are sent directly to the Data Layer, bypassing the ML.





### What has been done
Key changes:

- Raft-backed consumer membership, generations, and deterministic range assignments.
- Metadata invariant enforcing exactly one active owner per assigned range.
- Epoch seals dispatched to co-located data replicas after committed ownership changes.
- Persistent local offset ledger with restart recovery.
- Stale-generation rejection and future-generation request parking/unparking.
- Direct SDK-to-data-layer offset commits using replica quorum.
- Removed heartbeat topics and client-side ownership hash rings.
- SDK fencing recovery immediately halts ownership and resynchronizes with metadata.
- Split, merge, segment-roll, topic-delete, failover, and broker-restart handoffs covered.

- Assign sealed predecessors to exactly one descendant owner during lineage handoff.
- Start ancestors before descendants.
- Keep effective ownership changes controlled by the fetch manager.
- Pin the simulation RNG seed.
- Added lineage ownership regression tests.


Verification:

- cargo test --lib --all-features: 543 passed
- cargo clippy --all-targets --all-features -- -D warnings: passed

Primary additions: src/control_plane/metadata/consumer_group.rs and src/data_plane/offset_ledger.rs.