# EastGuard Frontend Simulator — Implementation Plan

## Goal

A browser-based interactive simulator that runs the **real EastGuard state machines** (Swim, Raft, Topology,
MetadataStateMachine) compiled to WebAssembly. A React/TypeScript frontend drives a virtual clock, renders cluster
state, animates in-flight packets, and exposes fault injection controls.

The simulator is a single free-play scenario with control knobs — no guided levels. The user starts a cluster, watches
it converge, then injects faults and observes recovery.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  Browser                                                │
│                                                         │
│  ┌────────────────┐   tick() / fault API                │
│  │  React Frontend │──────────────────────────────────► │
│  │  (TypeScript)   │                                    │
│  └────────────────┘ ◄──────────────────────────────────┤
│         ▲           SimSnapshot (JSON)                   │
│         │                                               │
│  ┌──────┴──────────────────────────────────────────┐    │
│  │  WASM Engine  (simulator/wasm)                   │    │
│  │                                                  │    │
│  │  SimulatorEngine                                 │    │
│  │    ├── VirtualNetwork  (packet queue + faults)   │    │
│  │    └── SimNode × N                               │    │
│  │          ├── Swim          (real EastGuard code)  │    │
│  │          ├── Topology      (real EastGuard code)  │    │
│  │          └── Raft × shards (real EastGuard code)  │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

The frontend owns nothing but rendering and input. All protocol logic lives in WASM.

---

## Repository Layout

```
simulator/
├── PLAN.md                        # this file
│
├── wasm/                          # Rust WASM crate
│   ├── Cargo.toml                 # wasm-bindgen, serde-json; depends on eastguard workspace
│   └── src/
│       ├── lib.rs                 # wasm-bindgen entry point; exports SimulatorEngine
│       ├── engine.rs              # top-level orchestrator
│       ├── sim_node.rs            # one physical node: owns Swim + Vec<Raft>
│       ├── virtual_network.rs     # packet queue, delay buckets, drop rules, partitions
│       └── snapshot.rs            # SimSnapshot and all sub-structs (serde Serialize)
│
└── frontend/                      # Vite + React app
    ├── package.json
    ├── vite.config.ts
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── wasm/
        │   └── bridge.ts          # typed TS wrapper over the raw WASM module
        ├── loop/
        │   └── SimLoop.ts         # requestAnimationFrame driver; speed control
        ├── store/
        │   └── simulation.ts      # Zustand store: SimSnapshot + UI state
        └── components/
            ├── TopBar/            # start/stop, speed slider, period counter
            ├── ClusterMap/        # SVG canvas: nodes, edges, packet animations
            │   ├── NodeCircle.tsx
            │   ├── EdgeLayer.tsx
            │   └── PacketAnimator.tsx
            ├── NodeInspector/     # slide-in panel on node click
            ├── HashRing/          # D3 arc ring: vnodes, color coding, topic routing
            └── FaultInjector/     # sidebar: kill/revive, partition, slow-link, drop
```

---

## Data Contract: SimSnapshot

Every `tick()` call returns one `SimSnapshot` — the complete serialized state the frontend renders from. The WASM layer
produces it; the frontend is read-only against it.

```typescript
interface SimSnapshot {
    tick: number;                      // virtual protocol-period counter

    nodes: NodeSnapshot[];
    packets_in_flight: PacketSnapshot[];
    hash_ring: HashRingSnapshot;
}

interface NodeSnapshot {
    id: string;
    // SWIM
    swim_state: "Alive" | "Suspect" | "Dead";
    incarnation: number;
    membership_table: MemberEntry[];   // this node's view of every peer
    active_probe: ProbeSnapshot | null;
    // Raft (one entry per shard group this node participates in)
    raft_groups: RaftGroupSnapshot[];
}

interface MemberEntry {
    node_id: string;
    state: "Alive" | "Suspect" | "Dead";
    incarnation: number;
}

interface ProbeSnapshot {
    target: string;
    phase: "Direct" | "Indirect" | "Suspect";
    seq: number;
    ticks_remaining: number;
}

interface RaftGroupSnapshot {
    shard_group_id: string;
    role: "Leader" | "Follower" | "Candidate";
    term: number;
    log_length: number;
    commit_index: number;
    last_applied: number;
    leader_id: string | null;
}

interface PacketSnapshot {
    id: string;            // unique; used as React key + animation id
    from: string;
    to: string;
    protocol: "UDP" | "TCP";
    label: string;         // "Ping seq=4", "AppendEntries term=2", etc.
    deliver_at_tick: number;
}

interface HashRingSnapshot {
    vnodes: VnodeSnapshot[];
    physical_nodes: PhysicalNodeColorEntry[];
}

interface VnodeSnapshot {
    hash: number;          // 0..u64, normalized to 0..1 for arc position
    owner_node_id: string;
    shard_group_id: string;
}
```

---

## WASM Engine — Public API (wasm-bindgen exports)

```rust
#[wasm_bindgen]
impl SimulatorEngine {
    // Lifecycle
    #[wasm_bindgen(constructor)]
    pub fn new(node_count: u8, shards_per_node: u8, seed: u64) -> SimulatorEngine;
    pub fn tick(&mut self) -> JsValue;          // → SimSnapshot as JSON
    pub fn reset(&mut self);

    // Fault injection
    pub fn kill_node(&mut self, node_id: &str);
    pub fn revive_node(&mut self, node_id: &str);
    pub fn partition(&mut self, group_a: JsValue, group_b: JsValue); // Vec<String>
    pub fn heal_partition(&mut self);
    pub fn set_link_delay(&mut self, from: &str, to: &str, delay_ticks: u32);
    pub fn clear_link_delay(&mut self, from: &str, to: &str);
    pub fn drop_next_packet(&mut self, from: &str, to: &str);       // one-shot

    // Application-level
    pub fn create_topic(&mut self, name: &str);
}
```

`tick()` advances the virtual clock by one protocol period:

1. Deliver all packets whose `deliver_at_tick <= current_tick` to their target `SimNode`
2. Call `sim_node.handle_tick()` on each node (drives `Swim` and `Raft` timers)
3. Drain new packets from every node's output buffers into `VirtualNetwork`
4. Apply fault rules (drop, delay, partition)
5. Serialize and return `SimSnapshot`

---

## VirtualNetwork — Packet Queue and Fault Model

```
VirtualNetwork
├── queue: BTreeMap<u64, Vec<InFlightPacket>>   // keyed by deliver_at_tick
├── drop_rules: HashSet<(NodeId, NodeId)>        // permanent drop until cleared
├── partition_a: HashSet<NodeId>
├── partition_b: HashSet<NodeId>                 // packets between A↔B dropped
├── link_delays: HashMap<(NodeId, NodeId), u32>  // extra ticks added on enqueue
└── one_shot_drops: HashSet<(NodeId, NodeId)>    // removed after first drop
```

When a packet is enqueued:

- If sender is Dead → drop
- If link is partitioned → drop
- If one-shot drop matches → drop and remove rule
- Otherwise: `deliver_at_tick = now + base_latency + link_delay`

Base latency = 0 ticks for simplicity initially (same-tick delivery). Can be bumped to 1–2 ticks for realism.

---

## Frontend Components

### TopBar

- EastGuard logo + "Simulator" label
- **Start / Stop** toggle (drives `SimLoop.running`)
- **Step** button (advance one tick when stopped)
- **Speed** slider: 1×, 5×, 20×, 100× (controls ticks-per-animation-frame)
- **Protocol Period** counter (displays `snapshot.tick`)
- **Reset** button

### ClusterMap (SVG)

- Nodes arranged in a circle (N evenly-spaced positions)
- **NodeCircle**
    - Fill: green (Alive), amber (Suspect), red (Dead)
    - Crown icon overlay when Raft leader for any shard group
    - Node ID label below
    - Click → opens NodeInspector for that node
- **EdgeLayer**
    - Line between every node pair (always visible, subtle gray)
    - Thickens / pulses when a packet is in flight on that edge
- **PacketAnimator**
    - One SVG `<circle>` per in-flight packet, interpolated along the edge
    - Blue dashed stroke = UDP (SWIM)
    - Purple solid stroke = TCP (Raft)
    - Small label next to the circle ("Ping", "AppEntries", etc.)
    - Packets consumed from snapshot; animation interpolates between ticks using rAF progress

### NodeInspector (slide-in right panel)

- Appears on node click; dismissible
- **SWIM Membership Table**: each peer → state badge + incarnation number
- **Active Probe**: target node, current phase (Direct → Indirect → Suspect), sequence number, ticks remaining (progress
  bar)
- **Raft Groups**: collapsible table — one row per shard group:
    - Shard group ID (short hash)
    - Role badge (Leader / Follower / Candidate)
    - Term, Log Length, Commit Index, Last Applied

### HashRing (bottom panel, collapsible)

- D3-drawn SVG arc ring
- Each arc segment = one vnode; color = owning physical node
- Physical node legend with color swatches
- Hover a vnode → tooltip: shard group ID, owner node, replica nodes
- **Topic Routing input**: type a topic name → animates a "routing ray" to the owning vnode segment and highlights the 3
  replica nodes

### FaultInjector (left sidebar)

Control panel sections:

**Node Controls** (requires selecting a node first on the canvas)

- Kill Node
- Revive Node

**Network Controls**

- Partition: two multi-select boxes (Group A / Group B) + "Partition" / "Heal" buttons
- Slow Link: from/to dropdowns + delay ticks input + Apply / Clear
- Drop Next Packet: from/to dropdowns + Drop button

**Live Indicators**

- Count of dropped packets this period
- Count of partitioned links
- Any dead nodes listed

---

## Implementation Phases

### Phase 1 — WASM Crate Setup (no UI)

**Goal:** `cargo build` and `wasm-pack build` succeed; `tick()` returns valid JSON.

Tasks:

1. Extract `east-guard-core` workspace crate: move pure state machine modules out of `east-guard`; confirm `east-guard`
   still compiles; confirm `east-guard-core` compiles with no `tokio`/`rocksdb` in its dependency tree
2. Create `simulator/wasm/Cargo.toml` — add `wasm-bindgen`, `serde`, `serde-json`, `getrandom` (`js` feature); depend
   on `east-guard-core`
3. Implement `VirtualNetwork` with enqueue / deliver / fault rules
4. Implement `SimNode` wrapping one `Swim` + `Topology` + `MetadataStateMachine` + `Vec<Raft>` (one per shard group)
5. Implement `SimulatorEngine::new(node_count, shards, seed)` — create nodes with only seed address knowledge; real SWIM
   bootstrap drives join via the virtual network
6. Implement `SimulatorEngine::tick()` — deliver packets → handle tick on each node → drain outputs → apply fault rules
   → serialize snapshot
7. Implement `SimSnapshot` + all sub-structs with `serde::Serialize`
8. Wire `wasm-bindgen` exports
9. Write a Rust unit test: run 200 ticks on a 3-node cluster, assert all nodes converge to Alive and at least one Raft
   leader exists per shard group

### Phase 2 — Frontend Scaffold

**Goal:** WASM loads in browser; `tick()` drives a console log every second.

Tasks:

1. `npm create vite@latest frontend -- --template react-ts`
2. Add `wasm-pack` output as a local package dependency
3. Write `bridge.ts`: async `loadEngine()`, typed wrappers over WASM
4. Write `SimLoop.ts`: rAF loop, speed multiplier, calls `engine.tick()` per frame
5. Write `simulation.ts` Zustand store: holds latest `SimSnapshot`
6. Minimal `App.tsx`: load engine → start loop → log snapshot tick count

### Phase 3 — ClusterMap

**Goal:** Nodes and edges visible; packet dots fly between nodes.

Tasks:

1. `ClusterMap.tsx`: SVG with computed node positions (trigonometry circle layout)
2. `NodeCircle.tsx`: SWIM state color + crown icon
3. `EdgeLayer.tsx`: thin lines between all pairs; pulse on active packets
4. `PacketAnimator.tsx`: interpolate packet position between source/target using
   `(now - emit_tick) / (deliver_tick - emit_tick)`; apply color/style by protocol

### Phase 4 — NodeInspector

**Goal:** Click node → panel shows SWIM table, active probe, Raft groups.

Tasks:

1. Store selected node ID in Zustand
2. `NodeInspector.tsx`: derive display data from `snapshot.nodes[selectedId]`
3. SWIM membership table with state badges
4. Probe progress bar (ticks_remaining / probe_timeout)
5. Raft groups table

### Phase 5 — HashRing

**Goal:** Ring renders; hover shows shard info; topic routing works.

Tasks:

1. Normalize vnode hashes to `[0, 2π]`
2. D3 arc per vnode, colored by owner node
3. Hover tooltip via SVG `onMouseEnter`
4. Topic input: hash the string in JS (same FNV-1a as the Rust impl), find the owning arc, animate highlight

### Phase 6 — FaultInjector

**Goal:** All fault injection controls call WASM and visual changes appear within one tick.

Tasks:

1. `FaultInjector.tsx`: sidebar with sections
2. Wire Kill / Revive to `engine.killNode()` / `engine.reviveNode()`
3. Partition UI: two multi-selects; "Partition" calls `engine.partition()`; "Heal" calls `engine.healPartition()`
4. Slow Link: dropdowns + `engine.setLinkDelay()`
5. Drop Next Packet: `engine.dropNextPacket()`

### Phase 7 — TopBar and Polish

**Goal:** Full interactive experience.

Tasks:

1. `TopBar.tsx`: Start/Stop, Step, Speed slider, tick counter, Reset
2. Speed slider: store ticks-per-frame; SimLoop calls `tick()` N times per rAF
3. Keyboard shortcut: Space = Start/Stop, `→` = Step
4. Responsive layout: top bar + main area (ClusterMap left, Inspector right) + collapsible HashRing bottom
5. Visual polish: dark theme, monospace font for counters, subtle grid background

---

## Dependency Notes

### Rust side

- `wasm-bindgen` for JS interop
- `serde` + `serde-json` for snapshot serialization (return `JsValue` from `tick()`)
- `getrandom` with `js` feature for seeded RNG in WASM
- `wasm-pack` as the build tool (`wasm-pack build --target web`)
- The WASM crate depends on `east-guard-core` (see Prerequisite below), NOT on the main `east-guard` crate

### JavaScript side

- `react`, `react-dom`, TypeScript
- `vite` + `@vitejs/plugin-react` + `vite-plugin-wasm` + `vite-plugin-top-level-await`
- `d3-shape`, `d3-path` for hash ring arcs
- `zustand` for state management
- `framer-motion` optional — CSS transitions may suffice for packet animation

---

## Prerequisite — Extract `east-guard-core` Workspace Crate

The main `east-guard` crate has `tokio`, `rocksdb`, and other native-only dependencies at the root. WebAssembly cannot
compile these. The pure state machines (`Swim`, `Raft`, `Topology`, `MetadataStateMachine`, `Ticker`) are async-free
and I/O-free, but they currently live in the same crate as the actors and storage layer that are not.

**Solution:** Create a new workspace crate `east-guard-core` containing only the pure state machines. No `tokio`, no
`rocksdb`, no async. The main `east-guard` crate becomes a thin layer of actors + storage that depends on
`east-guard-core`. The WASM crate also depends on `east-guard-core`.

```
Workspace
├── east-guard-core/     ← NEW: pure state machines only (no tokio, no rocksdb)
│   └── src/
│       ├── swim/        (moved from east-guard/src/clusters/swims/)
│       ├── raft/        (moved from east-guard/src/clusters/raft/)
│       ├── topology/    (moved from east-guard/src/clusters/swims/topology.rs)
│       ├── metadata/    (moved from east-guard/src/clusters/metadata/)
│       └── scheduler/   (moved from east-guard/src/schedulers/)
│
├── east-guard/          ← existing crate; actors + storage; depends on east-guard-core
│   └── src/
│       ├── clusters/    (actors, transport — keep tokio here)
│       └── ...
│
└── simulator/
    └── wasm/            ← depends on east-guard-core only
```

This refactor is the prerequisite for Phase 1. It is also a net improvement to the main codebase — the protocol logic
becomes independently testable without any async runtime.

**Shard group count:** Configurable by the user via the UI (default: 3 shards, 3 replicas). `SimulatorEngine::new()`
accepts `node_count` and `shards` as parameters.

**Bootstrap:** Real SWIM bootstrap — nodes start with knowledge of only their seed node addresses, then go through the
actual join handshake (JoinTry → Ping → Ack with gossip) driven by the virtual network and ticker. No synthetic
pre-population.

**MetadataStateMachine:** Included from the start. The hash ring shows real shard ownership derived from committed Raft
log entries applied to `MetadataStateMachine`. Topic creation (`create_topic()`) proposes a real `CreateTopic` command
through the real Raft path.

**Hash function parity (Q5):** Deferred — discuss when implementing Phase 5. Likely solution: expose
`engine.hash_key(name: &str) -> string` from WASM so JS never re-implements the hash.
