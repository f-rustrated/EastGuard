# Raft Transport (Invariants)

`RaftTransportActor` — async TCP transport for Raft RPCs. Manages persistent bidirectional connections between nodes. Each connection split into a reader task and a writer half held in the per-node `writers` map.

Separate from SWIM's UDP transport. Raft uses TCP for reliable, ordered delivery.

## Architecture (brief)

```
RaftTransportActor
├── listener (raft_port, TCP)
├── writers: HashMap<NodeId, OwnedWriteHalf>   (one write half per peer)
└── reader tasks (one per accepted/established connection)
```

## Wire Protocol

Length-prefixed bincode frames:
1. **Handshake** (first frame on every connection): `[len: u32][NodeId: bincode]`
2. **Messages**: `[len: u32][WireRaftMessage: bincode]`, where `WireRaftMessage` carries `shard_group_id` so transport can dispatch to the correct Raft group.

## Invariants

1. **Connection identity is established by handshake before any RPCs.** First frame on every connection is the initiator's `NodeId`. The acceptor uses this to key the writer slot and to detect the simultaneous-connect race. Without the handshake, the acceptor cannot route inbound messages to a peer-identified slot.

2. **At most one writer per peer.** `writers` is keyed by `NodeId`. Coexisting writers would split messages to the same peer across two TCP connections; per-connection ordering would let later messages overtake earlier ones in unpredictable patterns, causing the leader to chase its own retries.

3. **Lower NodeId wins on simultaneous connect.** When both sides connect concurrently, the acceptor drops the incoming connection if a writer for the peer already exists AND `peer_id > self.node_id`. Without this rule, both sides retain both connections (each thinks it won), violating invariant 2.

4. **Address resolution is always live.** Every connect attempt queries SWIM for the peer's current address; the transport keeps no local address cache. A stale local cache would connect to the wrong host after a peer moves or restarts on a different address.

5. **Frame sizes are bounded.** Handshake frames capped at 1KB; message frames at 4MB. Without bounds, a malicious or buggy peer can exhaust memory by sending a giant length prefix before any payload.

6. **Transport never inspects or modifies RPC payloads.** Wire messages are routed by their `shard_group_id` envelope; the payload bytes are opaque to the transport layer. Maintains layer separation — bugs in transport cannot corrupt consensus semantics.
