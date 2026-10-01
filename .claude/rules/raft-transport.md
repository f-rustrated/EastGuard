# Raft Transport (Rules)

`RaftTransportActor` is the async TCP transport for Raft RPCs. It manages one
persistent bidirectional connection per peer, owning its reader task and writer
half together. The cluster listener also serves one-shot ACL snapshot reads.
SWIM remains a separate UDP transport.

## Architecture

```
local certificates and trust roots
                |
                v
       mutual TLS + node IDs
                |
                v
          ClusterRequest
          |            |
          v            v
         Raft          ACL
   persistent peer     security actor -> quorum read -> reply -> close
```

## Wire Protocol

Length-prefixed Borsh frames:

1. In secure mode, TLS 1.3 authenticates both certificates. Both ends then send
   their `NodeId` and read the peer's `NodeId`. The shared
   `NodeTransportSecurity::exchange_node_identity` checks that the peer ID has
   the form `<certificate-principal>::<nonempty-suffix>`. The ID and principal
   must meet `MAX_SECURITY_ID_BYTES`; the frame is bounded before allocation.
2. The initiator checks the exact expected peer ID before sending application
   traffic. It then sends one `ClusterRequest`.
3. For `ClusterRequest::Raft`, the initial message identifies the sender.
   Later frames are raw `WireRaftMessage` values until close.
4. For `ClusterRequest::AclSnapshot`, the request carries one ACL resource.
   The server derives its shard locally and returns one `AclSnapshotResponse`.
5. Trusted-development mode skips TLS and the mutual identity exchange. Its
   first frame is directly a `ClusterRequest`.

There is no `ClusterHandshake`, process proof, or admission lookup endpoint.
These opening frames are incompatible with the previous development protocol.

## Rules

1. **Certificate authentication does not depend on Raft availability.**
   Certificates and trust roots are loaded locally. Node IDs must belong to
   the certificate's namespace, and outbound connections must match the exact
   intended process ID. No admission record or security actor query precedes
   Raft traffic. Two processes holding the same certificate and private key
   are equally trusted; the protocol does not fence them from each other.

2. **At most one live connection slot per peer.**
   `connections` is keyed by `NodeId` and owns the reader task and writer half.
   Replacing or removing a slot closes both halves. A reader-close event removes
   only its own generation, never a replacement.

3. **Lower NodeId wins on simultaneous connect.**
   An incoming connection loses when a locally initiated connection already
   exists and the local node ID is lower. A newer connection with the same
   origin replaces the older one.

4. **Address resolution is live.**
   Each connect attempt asks SWIM for the current address. The transport keeps
   no address cache; certificate and exact peer-ID checks reject a wrong target.

5. **Handshake and write work are bounded.**
   The listener caps its `JoinSet` before spawning and applies a total deadline.
   The actor collects verified streams directly from that set; completed tasks
   count toward the cap until collected. Slow TLS, identity exchange, or ACL
   reads do not block the listener's dispatcher loop.
   Outbound dial tasks and buffered messages are bounded. Write timeouts drop
   the whole connection because a partial frame cannot be safely reused.

6. **Frame sizes are bounded before allocation.**
   Identity, request, Raft, and response frames each have an explicit ceiling.

7. **Every Raft frame sender matches the connection peer.**
   A mismatch closes the connection. Transport routes by shard group but does
   not interpret the RPC. Existing voter, learner, leader, term, and log checks
   remain the Raft state machine's responsibility.

8. **ACL cache expiry does not expire Raft transport.**
   ACL records still require quorum-backed reads, but broker connections have
   no admission lease. Certificate expiry is currently checked at handshake
   only; active-session expiry and revocation remain production gates.

## ACL Snapshot Rule

An ACL snapshot request is not a Raft RPC. In secure mode, certificate
authentication and node-ID exchange precede it. The receiving broker derives
the shard locally and requires local Raft leadership and a quorum-backed read.
It returns the record on the same connection and closes.

An unavailable quorum fails the ACL read; it does not prevent independent Raft
connections from recovering the quorum. ACL reads cannot proxy client data or
mutate permissions. A Raft connection accepts only raw Raft frames after its
initial request.
