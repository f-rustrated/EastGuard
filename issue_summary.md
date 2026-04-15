# Issue #39: DS-RSM Foundation — Getting the System Up and Running

## Problem

RaftActor and RaftTransportActor existed but were not wired into `StartUp::run()`. Line 63 of `src/lib.rs` created `(raft_tx, _raft_rx)` and immediately dropped the receiver. SWIM membership events (`HandleNodeDeath`/`HandleNodeJoin`) fired into a dead channel — the SWIM→Raft pipeline was disconnected.

## Fixes

### 1. Wired RaftActor into StartUp (`src/lib.rs`)

- Created Raft channels: `raft_tx/raft_mailbox`, `raft_transport_tx/rx`, `raft_ticker_tx/rx`
- Bind `UdpSocket` and `TcpListener` on `cluster_port` before spawning (fail-fast on port conflicts). SWIM uses UDP, Raft uses TCP — no conflict on same port.
- Spawned a separate `run_scheduling_actor` for Raft timers (distinct `TTimer` type from SWIM)
- Spawned `SwimTransportActor::run()`, `RaftTransportActor::run()`, `SwimActor::run()`, `RaftActor::run()` — all as unit-like structs with channels passed directly

### 2. Filtered self-join membership events (`src/clusters/swims/actor.rs`)

`Swim::new()` registers the node itself as Alive, emitting a `MembershipEvent::NodeAlive` for its own ID. This caused `HandleNodeJoin` to fire for self, creating hundreds of single-member Raft groups (0 peers) — pure overhead. Fixed by filtering out membership events where `node_id == state.node_id` in `SwimActor::flush()`.

### 3. Reverse index for Topology (`src/clusters/swims/topology.rs`)

`shard_groups_for_node()` previously scanned every vnode position on the ring — O(total_vnodes x RF) per call. With 256 vnodes x 3 nodes = 768 positions, this was expensive and generated hundreds of shard groups per membership event.

Added a precomputed reverse index:
```
node_groups: HashMap<NodeId, HashMap<ShardGroupId, ShardGroup>>
```
- Rebuilt after every ring mutation (`insert_node`, `remove_node`)
- `shard_groups_for_node()` is now O(1) lookup
- Enables future delta-aware updates (only send changed groups to RaftActor)

### 4. E2E integration test (`src/it/e2e_cluster.rs`)

New test `e2e_swim_raft_cluster_lifecycle` verifies the full pipeline:
1. 3-node cluster forms via SWIM
2. Node-3 crashes → SWIM detects death → `HandleNodeDeath` flows to RaftActor
3. Node-3 restarts with fresh UUID → SWIM rejoin → `HandleNodeJoin` flows to RaftActor
4. All 3 nodes alive again — no panics throughout

### 5. Turmoil test configuration

All integration tests run with 256 vnodes and `tcp_capacity(4096)` to accommodate Raft TCP traffic, including crash-restart scenarios.

### 6. Raft transport dead-peer eviction (`src/clusters/raft/transport.rs`, `src/clusters/raft/actor.rs`, `src/clusters/raft/messages.rs`)

When a node dies and restarts at the same address with a new UUID, surviving nodes' `RaftTransportActor` held stale `addr_cache` entries and TCP connections to the old UUID. The restarted node got flooded with stale Raft RPCs, preventing SWIM rejoin.

Fix — three layers:
1. **`RaftTransportCommand::DisconnectPeer(NodeId)`** — new variant in transport command enum. RaftActor sends on `HandleNodeDeath`, transport clears `writers` + `addr_cache`.
2. **`dead_peers: HashSet<NodeId>`** — transport silently drops outbound RPCs to dead UUIDs, preventing reconnection attempts that would flood the restarted address.
3. **Periodic cleanup** — `dead_peers` set cleared every 5 minutes via `tokio::time::Interval` in the `select!` loop, preventing unbounded growth from accumulated dead UUIDs.
4. **Channel capacity 64→4096** for `raft_tx` — prevents SwimActor from blocking when sending `HandleNodeDeath` while hundreds of Raft timer callbacks queue.

All tests now run at 256 vnodes, including crash-restart scenarios.

### 7. Actor refactoring to match skill pattern

All four actors refactored to unit-like structs per `build-actor` skill:
- **SwimActor** — removed fields, channels passed directly to `run()`
- **RaftActor** — removed fields, `groups`/`seq_counter`/`shard_tokens` moved to local vars inside `run()`
- **SwimTransportActor** — removed fields, caller binds `UdpSocket` and passes it in
- **RaftTransportActor** — removed fields, caller binds `TcpListener` and passes it in

Socket/listener binding moved to `StartUp::run()` — fail-fast on port conflicts before any actor spawns.

---

## Issue: Nodes Marked Dead Immediately After Cluster Formation

### Symptom

Running a 3-node cluster per README instructions, nodes get marked `Suspect` then `Dead` within seconds of joining — even though all processes are healthy and reachable.

### Root Cause: Non-routable advertise address (`0.0.0.0`)

Default `--host` is `0.0.0.0` (bind all interfaces). When `--advertise-host` is not set, `advertise_peer_addr()` falls back to `host`, gossiping `0.0.0.0:<cluster_port>` as the node's address.

- **Bind** to `0.0.0.0` = listen on all interfaces ✓
- **Send** to `0.0.0.0` = non-routable, packets never delivered ✗

Initial join works because seed nodes use explicit `127.0.0.1` addresses from CLI args. But once gossip propagates the node's `advertise_addr` (`0.0.0.0:13001`), subsequent SWIM probes target that address and fail → direct probe timeout (300ms) → indirect probe → suspect → dead (5s).

### Fixes

1. **`src/config.rs` — `advertise_peer_addr()` now panics on unspecified address.** If the resolved advertise address is `0.0.0.0` (or `[::]`), startup fails with a clear error message instructing the operator to set `--advertise-host`.

2. **`README.md` — Added `--advertise-host 127.0.0.1`** to all three node commands in the cluster example.

3. **`src/clusters/swims/swim.rs` — Excluded self from `live_node_tracker`.** `Swim::new()` calls `update_member(self_node_id, Alive)` which added self to the probe rotation. `start_probe()` silently skips rounds when `next()` returns self, wasting 1 out of every N probe cycles. Fixed by guarding `live_node_tracker.update()` with `node_id != self.node_id`.
