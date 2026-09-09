# EastGuard Security Roadmap

**Goal:** Encrypt node and client traffic, reject unknown identities, and allow
only explicitly granted actions. Restarts and partitions must not let an old
process impersonate its replacement.

**Depends on:** SWIM, metadata Raft, data placement, and client routing.

Secure production startup is **not available yet**. It fails before opening
listeners. Plaintext requires the explicit `trusted-development` setting and
an isolated environment. The sections below describe the target; Section 7
separates working code from remaining work.

---

## 1. Boundary and Threats

Protect against network snooping, changed or replayed packets, forged senders,
stale processes, and attempts to fall back to plaintext. An authenticated client
still needs permission for each action. Compromised current brokers and
Byzantine consensus are outside this design.

| Listener | Default port | Target protection |
| --- | --- | --- |
| Client | TCP 2921 | TLS 1.3, client certificate, request ACL |
| Raft | TCP 2922 | TLS 1.3, node certificate, proof of the admitted process |
| Data | TCP 2923 | Same process checks as Raft |
| SWIM | UDP 2922 | Authenticated, encrypted, replay-protected datagrams; deferred |

A certificate is an identity, not permission. A redirect is an address hint,
not permission. Its destination must authenticate and authorize again.

## 2. Who Checks What

```
TLS and process proof
        |
        v
Security actor: read records, cache decisions, check ACLs
        |
        v
Application: check the operation and apply it
```

Transport checks the certificate, process proof, sender, frame size, and
connection deadline. Failure closes the connection. An ACL denial rejects one
request. Raft and data state machines still enforce their own rules: an admitted
broker does not automatically have leader, voter, or replica authority.
Replication and repair must use local committed placement, never a replica list
supplied by the sender. Raft peers and data replicas are different sets.
This is still a gap: a follower can currently create its local segment tracker
from the replica list in an append. It needs a committed placement source before
that path can enforce the target rule.

One security actor owns the admission and ACL caches. It reads local metadata
or tries the owning shard's replicas, starting with the known leader. Reads run
in the background. Identical pending reads share one result. Cache hits keep
working while other reads are slow.

Admission and ACL reads share a limit of 16 active reads. Each cache holds at
most 4,096 records, with separate byte limits; each pending read has at most 256
waiters. Full queues, unavailable owners, malformed records, and timeouts deny
the operation.

## 3. Node Identity and Admission

| Identity | Meaning |
| --- | --- |
| Certificate principal | Reusable broker name from exactly one `urn:eastguard:node:<principal>` URI in the certificate |
| Node ID | New identity for each process start |
| Process key | New signing key for that start; only the public key is stored in metadata |
| Admission epoch | Increasing number assigned when metadata replaces the admitted process |
| SWIM incarnation | Counter one process raises to refute stale liveness gossip |

The admission record is stored under `security/node/{principal}`. It names the
current epoch, node ID, and public key.

An operator with `security/cluster` permission approves a new process. The
owning Raft shard must assign the next epoch and revision atomically. Possession
of the reusable node certificate must never be enough to replace an admission:
an old process still has that certificate.

For a healed partition, the same process keeps its node ID, key, and epoch. It
raises its SWIM incarnation. A restart creates a new node ID and key and needs a
new admission. Epochs are compared only for the same certificate principal.
Within one admitted process, a higher incarnation wins; at equal incarnation,
Dead takes precedence over Suspect, then Alive.

SWIM reports liveness. It does not approve processes. Future secure SWIM must
check both the immediate sender and the admission of every relayed fact's
subject before that fact affects membership.

### Prove the process on each TCP connection

```
Connecting broker                       Accepting broker
       |------ mutual TLS --------------------|
       |------ signed process proof --------->|
       |                         check admission
       |<----- signed process proof ----------|
check admission                               |
       |------ application frames ------------|
```

Each signature covers the certificate principal, node ID, and a value derived
from that TLS connection. The receiver verifies it with the public key in the
current admission. A copied node ID or a signature from another connection is
not enough. Both sides prove admission before application traffic starts.

Use the exporter from the completed TLS handshake, with a label reserved for
EastGuard admission. Keep TLS early data disabled: it has weaker replay
protection. See [TLS 1.3, Sections 2.3 and 7.5](https://www.rfc-editor.org/rfc/rfc8446.html).

Every frame sender must match the verified peer. Outbound connections must
also match the intended destination's admitted node ID. Replacing a connection
closes its reader and writer together; an old reader cannot close a replacement.

### Record reads and expiry

Admission and ACL reads must prove current leadership through a Raft quorum.
Reading an old committed value from an isolated replica is not enough.

Cache entries expire at most 60 seconds after the read **started**, not after
the reply arrived. An entry also records its source shard and revision.
Changing the owner invalidates it. A confirmed missing record is a cacheable
denial; a failed read is not cached.

Raft and data connections stop reading and writing when the peer admission's
original deadline expires, including when idle or blocked. Reconnecting does
not extend a cached admission's life. This bounds continued acceptance of a
replaced process to 60 seconds, provided a fresh record can be obtained.

The Raft listener has one narrow exception to process admission: after mTLS, a
broker may read one admission record and then close. It cannot send Raft,
data, ACL, client, or mutation requests on that path. The serving node derives
the owner locally and answers only after its local Raft leader's quorum check.

This exception avoids recursive *connection authentication*, but does not
solve *quorum recovery*. A cold cluster, or one where all admission leases
expired during a partition, may need Raft connections to refresh the records
needed to open those same connections. Secure startup stays blocked until a
recovery design breaks this cycle without accepting stale admissions.
The pre-admission read also trusts the certificate-authenticated serving
broker; it is not a cryptographic proof that a quorum signed the returned record.

## 4. Client Permissions and Records

Client identity comes from exactly one
`urn:eastguard:client:<principal>` certificate URI. Grants are exact, with no
wildcards or inherited permissions. Unknown identities and missing grants deny.

| Resource | Permission |
| --- | --- |
| `cluster` | Create/list topics and inspect membership, topology, and ordinary diagnostics |
| `topic-admin/{topic-id}` | Describe or delete a topic |
| `topic-data/{topic-id}` | Produce, fetch, and read offset bounds |
| `consumer-group/{topic-id}/{group-id}` | Coordinate that group and read/commit its offsets |
| `security/cluster` | Manage ACLs, admissions, certificate revocations, and security audit |

Fetching group data also requires the topic-data grant. Producer-session
creation and renewal currently use topic-data permission and bind the session
to its creator. A separate producer-session resource is represented in the code
but is not currently checked; do not treat it as an enforced permission.

Admission, ACL, and revocation records belong to ordinary metadata shards.
Text paths select a shard; replicated records use typed resources. Each change
must commit through that shard's Raft group. Exact ACL reads and mutations come
before global listing, which needs pagination and partial-failure handling.

Authorize before returning placement or redirect information. A client should
send data directly to the correct data replica; brokers must not proxy client
data. A data replica must be able to authorize using a stable topic ID even when
it does not host that topic's metadata.

The existing name-based APIs do not fully satisfy this yet. Some discover the
metadata owner before checking an ACL, and produce still depends on local topic
metadata. Finish stable-ID routing and authenticated topic-name resolution
before calling the client boundary complete. Update the Rust client to use
mTLS for initial connections and every redirect.

## 5. Bootstrap, Rotation, and Recovery

Secure genesis is explicit cluster formation, not an automatic response to an
empty local directory. Initial members must agree on one immutable input:
initial membership, initial process admissions, and the first operator grant.
Persist that it was applied and reject conflicting input on restart.

Formation must also define how ordinary security shards recover ownership and
records as membership changes. Hashing a record path alone does not transfer its
state to a new owner.

| Operation | Required result |
| --- | --- |
| Process replacement | Separately authorized approval; Raft assigns epoch and revision; old process loses access by cache expiry |
| Leaf or CA rotation | Reload credentials online, with an overlap of old/new trust roots and no process identity change |
| Certificate revocation | Commit an issuer-and-serial record; reject new sessions and close matching active sessions within 60 seconds |
| Certificate expiry | Reject new sessions and stop existing ones by the expiry deadline, with an explicit bounded clock-skew policy |
| Recovery | Defined procedures for lost operator keys, accidental revocation, trust-root replacement, full restart, and partition healing |

Storing a revocation record does not enforce it. TLS credentials are currently
loaded once, and certificate lifetime is checked during the handshake. Online
reload, revocation checks, and active-session expiry still need implementation.

## 6. Resource Limits, UDP, and Audit

Bound work before spawning tasks or allocating frame payloads. Do not let a
slow handshake block a listener or a full mailbox grow an unbounded queue.
Client connections, handshakes, and concurrent requests need node-wide bounds
as well as a per-connection bound. These are capacity limits, not a
per-principal rate policy.

The client listener currently allows 1,024 connections, 128 pending TLS
handshakes, and 256 active requests across the node, with at most 32 handlers
per connection. TLS handshakes time out after 10 seconds, request handlers after
30 seconds, and response writes after 5 seconds. Data accepts at most 128
pending handshakes, each with a 16-second deadline. These initial limits need
load testing before production; they are not a throughput guarantee.

Per-principal request rates remain deferred until shared application identities
and autoscaling are defined. Keep security audit off the protocol's critical
path: use a bounded queue, count dropped events, and sample repeated failures.
Never log private keys, credentials, or message payloads.

### Secure SWIM remains a separate decision

Keep UDP's message boundaries and visible packet loss. Evaluate a maintained,
permissively licensed datagram-security implementation before selecting one.
The lack of a selected library is not evidence that every DTLS library is
unsuitable.

A transport must provide encryption, certificate identity, process-admission
binding, replay rejection, and a payload limit that avoids IP fragmentation.
It must run through EastGuard's UDP abstraction and use virtual time so turmoil
can test packet loss, retries, replay, and expiry deterministically.

A single cluster-wide key cannot identify an individual process. Network
isolation alone is development protection. Neither enables secure startup.

Bound handshake, session, and replay-tracking memory. Do not require *zero*
per-peer state: replay protection and secure sessions need state. A bounded
session cache is acceptable only if eviction cannot make captured old packets
valid again. Verify this explicitly. [DTLS 1.3](https://www.rfc-editor.org/rfc/rfc9147.html)
defines record sequencing, optional replay detection, and handshake
denial-of-service protections; an implementation must enable and test the
protections EastGuard requires.

## 7. Delivery Plan

Phase numbers name work areas, not a promise that they can ship independently.

| Phase | Current state | Complete when |
| --- | --- | --- |
| S0 — Configuration | Secure is default; credentials load; startup fails before listeners | Startup errors propagate and no secure configuration can open a plaintext listener |
| S1 — Records | Records are in snapshots; ACL grant/revoke apply internally; quorum-backed reads exist | Authorized wire operations cover ACLs, admission replacement, and revocation; log recovery and shard-ownership changes preserve them |
| S2 — Cluster TCP | Raft and data perform mutual process proof, bind senders, and enforce admission deadlines | Multi-node tests prove cold start, all leases expiring, partition healing, and data placement checks without a bootstrap cycle |
| S3 — SWIM | Deferred; development UDP is plaintext | A selected transport meets Section 6 and fences stale immediate and relayed identities |
| S4 — Clients | Server mTLS and ACL checks exist; request work is bounded | Rust-client mTLS, authorization before every redirect, and data-replica routing work for every API |
| S5 — Operations | Not complete | Genesis, safe read recovery, rotation, revocation, active-session expiry, and bounded audit work online |
| S6 — Production | Blocked | All earlier gates pass, including secure-SWIM simulation, replay/downgrade tests, malformed-frame tests, fuzzing, and measured resource bounds |

Do next:

1. Resolve admission/quorum recovery and genesis together. Test full restart and
   a partition longer than 60 seconds before enabling secure mode.
2. Add exact security administration with input bounds and server-assigned
   counters, then test durable recovery and ownership changes.
3. Finish client mTLS and authorization/routing, followed by credential lifecycle
   and audit.
4. Select and integrate secure SWIM, then run the production acceptance tests.

Keep secure startup closed until these gates pass. Passing TLS unit tests alone
does not establish a secure, recoverable cluster.
