# Raft Transport (Rules)

`RaftTransportActor` — async TCP transport for Raft RPCs. It manages persistent
bidirectional Raft connections between nodes. Each connection splits into a
reader task and a writer half held together in the per-node `connections` map.
The same authenticated cluster listener also serves a one-shot ACL snapshot read used to
refresh a broker's local authorization cache and a limited admission-record read used to authenticate a connecting process.

Separate from SWIM's UDP transport. Raft uses TCP for reliable, ordered delivery.

## Architecture (brief)

```
cluster listener (TCP)
        │
        ├── limited admission read ──► security actor → Raft → reply → close
        └── process proof ──► peer process proof ──► request
                                      │
                                      ├── Raft ──► one connection slot per peer
                                      └── ACL ───► security actor → Raft → reply → close
```

## Wire Protocol

Length-prefixed Borsh frames:

1. **Handshake:** `ClusterHandshake` carries exactly one of:
   - `Authenticate`, containing only the initiator's process proof;
   - `AdmissionLookup`, containing one certificate principal; or
   - `TrustedRequest`, containing a request only in trusted-development mode.
2. **Mutual admission:** on a secure request, the acceptor verifies the
   initiator and replies with its own `AdmissionProof`. The initiator verifies
   that proof before sending any `ClusterRequest`. Both proofs sign the same TLS
   exporter value and are checked against the peer's current admission record.
   The exporter lets both ends derive identical connection-specific bytes
   without sending those bytes; another TLS connection derives a different
   value.
3. **After mutual admission:**
   - The first Raft message carries its sender. Later frames are raw
     `WireRaftMessage` values until close. Each carries `shard_group_id`.
   - An ACL snapshot request carries only its resource. The accepting broker
     derives the current shard locally, serves it only when that shard is local,
     and returns one `AclSnapshotResponse` before closing.
4. **Trusted-development request:** no cryptographic admission exchange; the
   first request is carried directly inside `TrustedRequest`.

## Rules

1. **Secure connection identity comes from mTLS plus mutual process admission.**
   TLS supplies each stable Node Certificate Principal. A signature over the TLS
   exporter value proves possession of the current process key and cannot be
   replayed on another TLS session. Mutual proof is required because the Raft
   connection carries traffic in both directions. The Raft sender or ACL requester
   must equal its admitted `NodeId`. The first Raft sender then keys the writer slot
   and detects the simultaneous-connect race.

2. **At most one live connection slot per peer.** `connections` is keyed by
   `NodeId` and owns the matching reader task and writer half. Replacing or
   removing a slot closes both halves. A reader-close event removes only its
   own generation, so an old reader cannot tear down its replacement.

3. **Lower NodeId wins on simultaneous connect.** When both sides connect
   concurrently, the acceptor drops the incoming connection if a locally
   initiated connection for the peer already exists AND
   `peer_id > self.node_id`. Without this rule, both sides retain both
   connections (each thinks it won), violating rule 2.

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

8. **Admission leases bound persistent traffic.** A successful admission lookup
   carries the original cache deadline. Reader dispatch and writer operations
   stop no later than that deadline, forcing a new quorum-backed admission read
   before traffic can resume. A handshake never starts a new 60-second validity
   window.

## Limited Admission Lookup Rule

Admission records are sharded, so the acceptor may need another broker to read
the record required for its proof check. Requiring process admission for that
read would recurse. `AdmissionLookup` is therefore accepted after mTLS but
before process admission. It carries one certificate principal; the accepting
broker derives the current shard and serves the read only when that shard is
local and its Raft instance proves current leadership with a quorum-backed read
barrier. The endpoint returns one `AdmissionLookupResponse` and closes. It
cannot carry Raft, ACL, client, or admission-write traffic.

## ACL Snapshot Rule

An ACL snapshot request is not a Raft RPC and never enters a Raft state machine.
In secure mode it is served only after the requester completes process
admission. The accepting broker derives the selected shard from the resource;
the broker security actor asks the local multi-Raft leader for a quorum-backed
ACL record, returns that record on the same connection, then closes. It carries
no client data request and cannot proxy one.
A connection admitted for Raft carries only raw Raft frames after its first
message; an invalid frame closes the connection.
