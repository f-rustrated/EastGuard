# Raft Transport (Invariants)

`RaftTransportActor` — async TCP transport for Raft RPCs. It manages persistent
bidirectional Raft connections between nodes. Each connection splits into a
reader task and a writer half held in the per-node `writers` map. The same
authenticated cluster listener also serves a one-shot ACL snapshot read used to
refresh a broker's local authorization cache.

Separate from SWIM's UDP transport. Raft uses TCP for reliable, ordered delivery.

## Architecture (brief)

```
cluster listener (TCP)
        │
        ├── initial Raft message ──► persistent reader + one writer per peer
        │
        └── initial ACL request ──► read committed ACL → reply → close
```

## Wire Protocol

Length-prefixed Borsh frames:
1. **Initial message** (first frame on every connection): either a Raft message
   or an ACL snapshot request.
   - The initial Raft message carries its sender, which establishes the peer
     identity, then later frames are raw `WireRaftMessage` values until close.
     Each message carries `shard_group_id` so transport can dispatch it to the
     correct Raft group.
   - The initial ACL snapshot request carries its requesting node, shard, and
     resource. Its response is one `AclSnapshotResponse`, then the connection
     closes.

## Invariants

1. **Connection identity is established by the initial Raft message.** Its
sender keys the writer slot and detects the simultaneous-connect race. Without
it, the acceptor cannot route later frames to a peer-identified slot.

2. **At most one writer per peer.** `writers` is keyed by `NodeId`. Coexisting writers would split messages to the same peer across two TCP connections; per-connection ordering would let later messages overtake earlier ones in unpredictable patterns, causing the leader to chase its own retries.

3. **Lower NodeId wins on simultaneous connect.** When both sides connect concurrently, the acceptor drops the incoming connection if a writer for the peer already exists AND `peer_id > self.node_id`. Without this rule, both sides retain both connections (each thinks it won), violating invariant 2.

4. **Address resolution is always live.** Every connect attempt queries SWIM for the peer's current address; the transport keeps no local address cache. A stale local cache would connect to the wrong host after a peer moves or restarts on a different address.

5. **Frame sizes are bounded.** Every initial, Raft, and ACL response frame is
capped at 4MB. Without bounds, a malicious or buggy peer can exhaust memory by
sending a giant length prefix before any payload.

6. **Transport validates message identity but never interprets the RPC.** The
connection peer must match the message `sender`; a mismatch closes that
connection. The transport routes by `shard_group_id` and passes the authenticated
peer onward, but the RPC remains opaque. Voter, learner, leader, term, and log
checks belong to the target Raft state machine.

## ACL Snapshot Rule

An ACL snapshot request is not a Raft RPC and never enters a Raft state machine.
It asks the local multi-Raft actor for the selected shard's committed ACL record,
returns that record on the same authenticated connection, then closes the
connection. It carries no client data request and cannot proxy one. A cluster
connection that begins with a Raft message carries only raw Raft frames after
that message; an invalid frame closes the connection.
