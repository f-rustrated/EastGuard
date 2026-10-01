# Raft Transport (Rules)

`RaftTransportActor` is the async TCP transport for Raft RPCs. It manages one
persistent bidirectional connection per peer, owning its reader task and writer
half together. The cluster listener serves Raft messages only.
SWIM remains a separate UDP transport.

## Architecture

```
local certificates and trust roots
                |
                v
       mutual TLS + node IDs
                |
                v
         WireRaftMessage
                |
                v
       persistent Raft peer
```

## Wire Protocol

Length-prefixed Borsh frames:

1. In secure mode, TLS 1.3 authenticates both certificates. Both ends then send
   their `NodeId` and read the peer's `NodeId`. The shared
   `NodeTransportSecurity::exchange_node_identity` checks that the peer ID has
   the form `<certificate-principal>::<nonempty-suffix>`. The ID and principal
   must meet `MAX_SECURITY_ID_BYTES`; the frame is bounded before allocation.
2. The initiator checks the exact expected peer ID before sending application
   traffic. It then sends a `WireRaftMessage`.
3. The initial message identifies the sender. All later frames use the same
   `WireRaftMessage` format until close.
4. Trusted-development mode skips TLS and the mutual identity exchange.

There are no admission or ACL lookup endpoints. The obsolete development wire
wrapper and security metadata formats have been removed; no upgrade compatibility
is promised before the first deployment.

## Rules

1. **Certificate authentication does not depend on Raft availability.**
   Certificates and trust roots are loaded locally. Node IDs must belong to
   the certificate's namespace, and outbound connections must match the exact
   intended process ID. No metadata query precedes Raft traffic. Two processes holding the same certificate and private key
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
   count toward the cap until collected. Slow TLS or identity exchange does not block the listener's dispatcher loop.
   Outbound dial tasks and buffered messages are bounded. Write timeouts drop
   the whole connection because a partial frame cannot be safely reused.

6. **Frame sizes are bounded before allocation.**
   Identity, request, Raft, and response frames each have an explicit ceiling.

7. **Every Raft frame sender matches the connection peer.**
   A mismatch closes the connection. Transport routes by shard group but does
   not interpret the RPC. Existing voter, learner, leader, term, and log checks
   remain the Raft state machine's responsibility.

8. **Credential lifecycle belongs to the deployment.**
   Certificate validity is checked at handshake. Credential replacement and
   access withdrawal require operator procedures that close existing sessions.
   Static client permissions have no role in broker-to-broker authentication.
