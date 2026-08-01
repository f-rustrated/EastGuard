# Raft Transport (Invariants)

`RaftTransportActor` — async TCP transport for Raft RPCs. It manages persistent
bidirectional Raft connections between nodes. Each connection splits into a
reader task and a writer half held in the per-node `writers` map. 
The same authenticated cluster listener also serves a one-shot ACL snapshot read used to
refresh a broker's local authorization cache and a limited admission-record read used to authenticate a connecting process.

Separate from SWIM's UDP transport. Raft uses TCP for reliable, ordered delivery.

## Architecture (brief)

```
cluster listener (TCP)
        │
        ├── limited admission read ──► security actor → Raft → reply → close
        │
        └── request with process proof
              │
              ├── both sides verify a TLS-session-bound process proof
              ├── Raft ──► persistent reader + one writer per peer
              └── ACL ───► security actor → Raft → reply → close
```

## Wire Protocol

Length-prefixed Borsh frames:

1. **Secure initial message:** either `AdmissionLookup(AdmissionRecordKey)` or
   `ProcessAdmission(AdmissionRequest)`. The admission request contains the
   dialer's process proof and one `ClusterRequest`: Raft or ACL snapshot.
2. **Mutual admission:** the acceptor verifies the dialer, then replies with its
   own `AdmissionProof`. Both proofs sign the same TLS exporter value and are
   checked against the peer's current admission record. The exporter lets both
   ends derive identical connection-specific bytes without sending those bytes;
   another TLS connection derives a different value.
3. **After mutual admission:**
   - The first Raft message carries its sender. Later frames are raw
     `WireRaftMessage` values until close. Each carries `shard_group_id`.
   - An ACL snapshot request carries its requesting node, shard, and resource.
     Its response is one `AclSnapshotResponse`, then the connection closes.
4. **Trusted-development initial message:** no cryptographic admission exchange;
   `Request(ClusterRequest)` directly carries the Raft or ACL snapshot request.

## Invariants

1. **Secure connection identity comes from mTLS plus mutual process admission.**
   TLS supplies each stable Node Certificate Principal. A signature over the TLS
   exporter value proves possession of the current process key and cannot be
   replayed on another TLS session. Mutual proof is required because the Raft
   connection carries traffic in both directions. The Raft sender or ACL requester
   must equal its admitted `NodeId`. The first Raft sender then keys the writer slot
   and detects the simultaneous-connect race.

2. **At most one writer per peer.** `writers` is keyed by `NodeId`. Coexisting writers would split messages to the same peer across two TCP connections; per-connection ordering would let later messages overtake earlier ones in unpredictable patterns, causing the leader to chase its own retries.

3. **Lower NodeId wins on simultaneous connect.** When both sides connect concurrently, the acceptor drops the incoming connection if a writer for the peer already exists AND `peer_id > self.node_id`. Without this rule, both sides retain both connections (each thinks it won), violating invariant 2.

4. **Address resolution is always live.** Every connect attempt queries SWIM for the peer's current address; the transport keeps no local address cache. A stale local cache would connect to the wrong host after a peer moves or restarts on a different address.

5. **Handshake work is bounded.** The listener acquires a permit before
spawning a handshake task, applies a total handshake deadline, and uses a
bounded queue to return verified Raft streams to the dispatcher. A slow TLS,
admission lookup, or proof exchange never blocks the transport select loop.

6. **Frame sizes are bounded.** Initial, process-admission, Raft, proof, and response
frames are capped before allocating their payload.

7. **Transport validates message identity but never interprets the RPC.** The
connection peer must match the message `sender`; a mismatch closes that
connection. The transport routes by `shard_group_id` and passes the authenticated
peer onward, but the RPC remains opaque. Voter, learner, leader, term, and log
checks belong to the target Raft state machine.

## Limited Admission Lookup Rule

Admission records are sharded, so the acceptor may need another broker to read
the record required for its proof check. Requiring process admission for that
read would recurse. `AdmissionLookup(AdmissionRecordKey)` is therefore accepted
after mTLS but before process admission. It can read one named admission record
from one shard, returns one `AdmissionLookupResponse`, and closes. It cannot
carry Raft, ACL, client, or admission-write traffic.

## ACL Snapshot Rule

An ACL snapshot request is not a Raft RPC and never enters a Raft state machine.
In secure mode it is served only after the requester completes process
admission. It asks the local multi-Raft actor for the selected shard's committed
ACL record through the broker security actor, returns that record on the same
connection, then closes. It carries no client data request and cannot proxy one.
A connection admitted for Raft carries only raw Raft frames after its first
message; an invalid frame closes the connection.
