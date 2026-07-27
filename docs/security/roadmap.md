# EastGuard Security Roadmap

**Goal:** Make secure deployment the default without preventing online rotation,
node restart, or recovery from a network partition.

EastGuard will provide:

1. Cryptographic identity for every node and client.
2. TLS 1.3 for TCP and DTLS 1.3 for SWIM UDP.
3. Default-deny authorization on every broker.
4. Safe node admission, certificate-ID reuse, rotation, revocation, and audit.

**Depends on:** SWIM membership, Raft consensus, data-plane placement, the client
protocol, and client redirect handling.

---

## Current Boundary

EastGuard currently trusts anything that can reach its listeners. Message senders
declare identities but do not prove them.

| Port | Traffic |
|---|---|
| TCP 2921 | Client metadata, administration, produce, and fetch |
| TCP 2922 | Raft |
| TCP 2923 | Data replication, repair, and coordination |
| UDP 2922 | SWIM |

Until this roadmap is complete, production deployment is restricted to an
isolated trusted network.

---

## Threat Model

The production boundary protects against a network attacker who can connect,
spoof UDP, inspect or modify traffic, replay messages, present invalid
credentials, and exhaust handshake resources.

EastGuard prevents that attacker from:

- impersonating a client or node;
- joining SWIM or a Raft group;
- poisoning addresses or membership facts;
- performing unauthorized administration, produce, or consume operations;
- injecting replication or repair messages;
- downgrading a secure connection to plaintext.

A compromised broker or authorized client is outside this boundary. Scoped,
short-lived, revocable credentials limit damage but do not make a compromised
principal trustworthy. Byzantine consensus is not in scope.

---

## Security Modes

| Mode | Use | Behavior |
|---|---|---|
| **Secure** | Production and shared networks | Credentials required; plaintext rejected |
| **Trusted development** | Isolated development and tests | Existing plaintext protocols enabled by explicit opt-in |

Secure mode is the default and never downgrades. Startup fails before opening a
listener when keys, certificates, trust roots, identity bindings, or security
settings are invalid. Secure and trusted-development participants cannot
communicate.

---

## Communication and Application Boundaries

### Protocols

| Listener | Protection | Peer authentication |
|---|---|---|
| Client TCP 2921 | TLS 1.3 | Mutual X.509 |
| Raft TCP 2922 | TLS 1.3 | Mutual X.509 |
| Data TCP 2923 | TLS 1.3 | Mutual X.509 |
| SWIM UDP 2922 | DTLS 1.3 | Mutual X.509 |

TLS 1.2, DTLS 1.2, anonymous modes, plaintext, and downgrade are rejected in
secure mode. DTLS uses replay protection and a stateless cookie before certificate
verification or per-peer allocation.

S0 must prove that a maintained DTLS 1.3 implementation supports certificates,
cookies, anti-replay, retransmission, and bounded state. If not, secure production
delivery is blocked rather than silently falling back to DTLS 1.2. DTLS 0-RTT is
disabled. Cookie keys rotate, handshake retries and associations are bounded, and
address changes require revalidation.

### Layer split

```
TLS or DTLS
  authenticate peer
  enforce framing and resource limits
  bind envelope sender to peer identity
                  |
                  v
application
  authorize the target group, segment, or client operation
```

Certificate checks, signatures, admission reads, and cache refresh happen at the
network actor boundary. Synchronous SWIM, Raft, topology, and data-plane state
machines receive only validated facts and perform no security I/O.

Transport closes a connection for authentication, framing, or envelope-identity
failure. An application authorization failure drops only that envelope, so valid
traffic for other groups and segments continues on the shared connection.

Raft authorization follows
[D8 — Raft RPC Sender Authorization](../metadata-management/d8_raft_rpc_sender_authorization.md).
Metadata Raft remains the placement authority. Each data-plane node caches only
the committed placement needed for its local segments and uses that cache—not a
replica list supplied by an incoming message—to authorize replication, repair,
and coordination.

Clients authenticate every redirect destination against the configured trust
root. The destination authenticates and authorizes the client again; a redirect
grants no authority.

---

## Identity

### Node lifecycle identity

Security adds two terms around EastGuard's existing `NodeId` and SWIM
incarnation:

| Term | Existing or new | Meaning |
|---|---|---|
| **Certificate node ID** | New | Operator-chosen identity reused across restarts |
| **Admission epoch** | New | Restart number committed by metadata; a higher number replaces an older process |
| **NodeId** | Existing | Lifecycle-specific ID used by SWIM, topology, Raft, and data placement |
| **SWIM incarnation** | Existing | Conflict counter increased by the same process after a healed partition |

The counters answer different questions:

```
same process returns after partition
        |
        +-- same NodeId and admission epoch
        +-- increase SWIM incarnation

process restarts or is replaced
        |
        +-- metadata commits a higher admission epoch
        +-- process starts with a new NodeId
        +-- reject the older process
```

The admission record connects the reusable certificate identity to one running
process:

```
certificate node ID A
        |
        +-- admission epoch 8
        +-- NodeId A::7f2c...
```

Each restart receives a higher epoch and keeps EastGuard's newly generated
`NodeId`. The certificate proves the certificate node ID. The first application
frame presents the epoch and `NodeId`; the admission record must contain that
exact pair. The envelope sender must then match the admitted `NodeId`. A peer
accepts a new node connection only after a fresh read from the metadata shard
that owns the admission record.

SWIM, ring placement, Raft membership, connection ownership, and data replica
sets continue to use `NodeId`. A restart therefore remains a new protocol
identity, matching EastGuard's current safety model.

### Partition failback and restart

Certificate node-ID reuse is supported in two forms:

- **Healed partition:** The process keeps its `NodeId` and admission epoch. It
  increases its SWIM incarnation to refute stale `Suspect` or `Dead` gossip.
- **Restart or replacement:** The process receives a new admission epoch and
  `NodeId`. The newer epoch fences every older process, regardless of its SWIM
  incarnation.

Identity facts are ordered by admission epoch first and SWIM incarnation second:

```
higher admission epoch wins
same epoch -> higher incarnation wins
same incarnation -> Dead > Suspect > Alive
```

An old partitioned process cannot return after a replacement and reclaim its
certificate node ID. Its epoch is stale, so its sessions and gossip are rejected.

### SWIM admission

DTLS authenticates and protects each hop. SWIM continues to relay membership
facts using its existing `NodeId`, incarnation, and state ordering. Per-fact
signatures are unnecessary because compromised brokers are outside the threat
model.

**Gossip rule:** Process a SWIM fact only while its `NodeId` and epoch match a
current admission in the broker's cache. Then apply the existing SWIM ordering.

Admission and SWIM have separate authority:

- Admission decides whether a lifecycle identity may participate.
- SWIM decides whether that admitted identity appears alive.
- Raft membership requires both admitted and alive.

A SWIM event cannot introduce an unadmitted `NodeId`. Revocation or a newer
admission epoch fences the old process immediately on brokers that observe the
commit and everywhere else within the admission-cache deadline. Invalidation
closes the old process's sessions and removes its SWIM facts, then drives the
same Raft-removal path as confirmed death.

### Client identity

A client certificate maps to one immutable principal for the connection.
Request IDs, producer IDs, consumer-group member IDs, and topic names are
application data, not credentials.

A producer session is permanently bound to the principal that created it.
Requests from another principal are rejected before sequence, fencing, expiry, or
deduplication state changes.

---

## Authorization

Secure mode uses exact, default-deny grants. There are no wildcards, inheritance,
or payload-defined resources in the first production version.

| Exact key | Actions |
|---|---|
| `cluster` | Inspect membership and shards; operator/debug actions |
| `topic-admin/{topic}` | Create, delete, describe |
| `topic-data/{topic}` | Produce, fetch, list offsets |
| `consumer-group/{topic}/{group}` | Consume; inspect or reset progress |
| `producer-session/{topic}/{session}` | Renew |
| `security/cluster` | Read/change ACLs, admit or revoke identities, inspect audit |

`Consume` covers join, heartbeat, leave, assignment, and offset read/commit.
Reading records also requires `Fetch` on the topic. Offset inspection or reset is
a separate administrative grant. Opening a producer session requires `Produce`
on its topic; the created session key is then bound to that principal. Listing
topics filters the result to topics for which the principal has an exact
`topic-admin/{topic}` or `topic-data/{topic}` grant.

A client certificate contains one opaque principal ID. Resources are typed values
and comparison is exact. Topic creation checks the requested topic name. One
security-record update changes one resource atomically. Only a principal with
change access to `security/cluster` may grant or remove access; operator recovery
handles accidental loss of the last administrator.

### Sharded security records

EastGuard has no dedicated security group or controller. Security records use the
existing sharded metadata system:

| Key | Value |
|---|---|
| `security/node/{certificate-node-id}` | Current admission epoch and `NodeId` |
| `security/acl/{resource}` | ACL |
| `security/revocation/{issuer}/{serial}` | Credential revocation |

Each key hashes to an ordinary metadata shard and commits through that shard's
Raft log. Any broker can resolve the owner. If it does not host the shard, it
returns a redirect; the requester retries at the target.

```
requester -> any broker -> hash security key
                           |
                 broker hosts shard?
                    |             |
                   no            yes
                    |             |
                 redirect      propose or
                    |          leader redirect
                    v             |
             requester retries    v
                              Raft commit
```

This adds security records to each metadata state machine, including replicated
commands, queries, snapshots, recovery, and cache-update events. It is new
metadata functionality, not a generic key-value capability that exists today.

Brokers authorize locally from versioned, deadline-bound snapshots of these
records. The broker executing an operation always repeats the check, including
after redirects and retries.

Versions are per security key. An authorization decision reads one exact ACL
record. Admission and revocation checks for a new connection require a fresh read
from the owning shard; an unavailable owner fails closed. Existing sessions may
use cached ACL, admission, and revocation state until its deadline.

The production maximum cache age is 60 seconds. Operators may shorten it, not
extend it. Each effective deadline is the earliest of the record expiry,
certificate expiry, and local receipt time plus that maximum. Elapsed cache age
uses a monotonic timer. A backward wall-clock jump beyond the configured
tolerance invalidates the cache instead of extending it.

---

## Bootstrap and Join

Bootstrap starts from operator-provided trust material:

1. Create a cluster-specific trust root.
2. Issue the first node certificate.
3. Put the first node record, the first administrator principal, and that
   principal's `security/cluster` change grant in the initial metadata state.
4. Start the first node in secure mode.
5. Authenticate a later candidate's certificate on the limited pre-admission
   endpoint.
6. Resolve `security/node/{certificate-node-id}` and follow redirects to its
   metadata shard.
7. Atomically commit the next admission epoch and `NodeId`.
8. Allow that `NodeId` to enter SWIM and Raft membership flows.

Knowing a seed address grants no authority. A candidate cannot change SWIM,
topology, or Raft state before admission commits.

The initial administrator grant is auditable and can be replaced through normal
ACL updates; it does not bypass Raft after startup. Operator recovery is the
quorum-protected offline path if the last administrator is lost.

The pre-admission endpoint also breaks recovery cycles after a cold restart. A
certificate-authenticated broker may use it only to read admission or revocation
records and receive owner redirects. It grants no SWIM, Raft, data, ACL-mutation,
or client authority. A candidate may additionally submit the one admission
command for its own pre-authorized certificate node ID. Later candidates need not
belong to a metadata shard: a broker returns the owner redirect, and the
candidate retries there.

Reusing a certificate node ID is a new admission, not an implicit resurrection.
The new epoch must commit before the replacement joins. The restarted process
does not choose an epoch: the owning metadata shard reads the current record and
commits the next value. If that shard lacks quorum, admission waits.

A valid node certificate may renew only its own existing certificate node ID.
Adding a previously unseen certificate node ID requires a `security/cluster`
change grant. Committing a replacement always fences the active process.
Admission attempts are rate-limited per certificate node ID to prevent epoch
churn.

---

## Credential Lifecycle

### Rotation

1. Add the new trust chain.
2. Reload trust while the old chain remains valid.
3. Issue and reload new leaf certificates.
4. Reconnect long-lived sessions.
5. Remove the old chain and verify rejection.

Nodes reload certificates and trust without restarting. Rotation does not require
a cluster-wide outage. Reloading a certificate does not change the admission
epoch or `NodeId`; the admission record binds the certificate node ID and issuer
policy rather than one leaf fingerprint.

### Expiry and revocation

Expired credentials are rejected after a bounded clock-skew allowance. EastGuard
warns and emits metrics before expiry.

Brokers cache versioned security snapshots with absolute deadlines:

| Operation while the metadata shard owning the required security record is unavailable | Policy |
|---|---|
| New client/node connection, join, membership, ACL, revocation, admin | Fail closed immediately |
| Existing data traffic and required replication | Continue until snapshot expiry, then fail closed |
| Existing cluster control connection | Reject known revocations; otherwise continue until snapshot expiry |

Known revocations close established connections within the enforcement deadline.
Cached policy is never used after its effective deadline.

Operations documentation covers lost keys, issuing-key loss, accidental
revocation, expiry, loss of quorum in a metadata shard that owns security records,
trust-root replacement, and certificate-node-ID recovery. Recovery never enables
plaintext.

---

## Resource Limits and Audit

Every listener bounds connections, unauthenticated handshakes, attempts per
source, deadlines, certificate and frame sizes, in-flight requests, tasks,
buffers, and event rates. Invalid DTLS packets are dropped before SWIM decoding.
Connection, handshake, and request permits are acquired before frame allocation
or task creation. Aggregate byte limits apply across connections, not only to
individual frames.

Every encrypted SWIM application-data record fits in one UDP datagram. Its UDP
payload is limited to 1200 bytes by default, leaving room for IPv6 and UDP headers
within the 1280-byte minimum IPv6 MTU. The gossip budget is what remains after
DTLS and the fixed SWIM envelope. Oversized application packets are rejected
before sending or decoding. Certificate handshakes may span bounded DTLS
datagrams.

Security audit events record the reporting node, normalized principal, endpoint,
operation, resource, result, stable reason code, correlation ID, and a non-secret
certificate fingerprint when useful.

Audit emission uses:

- bounded detail queues;
- bounded counters by reason;
- sampled details and periodic aggregate summaries during floods;
- an overflow counter and recovery summary.

Audit backpressure never blocks client, membership, consensus, or replication
work. Audit output never contains keys, secrets, raw credentials, session keys, or
request payloads.

---

## Delivery

| Phase | Build | Exit result |
|---|---|---|
| **S0** | Modes, configuration, key loading, DTLS feasibility | Secure mode opens no plaintext listener; DTLS choice is proven viable |
| **S1** | Sharded security records, snapshots, initial node and administrator records | Security state survives commit, migration, snapshot, and recovery |
| **S2** | Cluster TCP mTLS, admission fencing, envelope binding, Raft D8, data-plane authority | Authenticated cluster traffic |
| **S3** | DTLS and the SWIM admission/liveness gate | Safe membership and partition failback |
| **S4** | Client mTLS, principals, ACL enforcement | Every client operation is default-deny |
| **S5** | Reload, rotation, expiry, revocation, recovery, audit | Online credential operations |
| **S6** | Adversarial, fuzz, partition, and load tests | Production readiness gate passes |

Delivery is linear: S0 → S1 → S2 → S3 → S4 → S5 → S6.

---

## Validation

### Identity and transport

- Use credentials signed by an untrusted root or issued for another node.
- Mismatch certificate node ID, admission epoch, `NodeId`, or envelope sender.
- Replay an older admission epoch after a replacement joins.
- Heal a partition and verify the same process refutes stale death with a higher
  incarnation.
- Start a replacement and verify its higher epoch fences the old partitioned boot.
- Verify fencing is immediate where the new epoch is observed and occurs within
  60 seconds everywhere else.
- Modify or replay DTLS-protected SWIM packets.
- Attempt plaintext and protocol downgrade on every listener.
- Verify encrypted SWIM UDP payloads stay within 1200 bytes and oversized inbound
  or outbound packets are dropped without IP fragmentation.
- Measure encoded identity and failure-report sizes, facts per datagram,
  admission-cache misses, and convergence time at expected and maximum cluster
  sizes.
- Fuzz handshakes, datagrams, routing envelopes, and frame decoders.

### Application authority

- Run the Raft D8 authorization suite.
- Send data-plane messages from outside committed placement.
- Verify one rejected group or segment does not disrupt others on the connection.
- Test every client action with no grant, an unrelated exact grant, and the
  required exact grant.
- Use one principal's producer session from another principal.
- Verify redirects and retries repeat authorization.

### Operations and load

- Rotate and revoke credentials during elections, produce, fetch, replication,
  and repair.
- Partition brokers from metadata shards owning required security records before
  and after snapshot expiry.
- Cold-restart every broker with empty caches and no established sessions.
- Test expiry, clock skew, recovery, handshake floods, audit floods, and resource
  bounds.
- Measure reconnect storms and metadata-shard partitions at shorter cache ages.
- Verify diagnostics disclose no credential material.

---

## Production Readiness Gate

Production deployment requires:

1. Secure mode is the default and cannot downgrade.
2. TLS protects TCP and DTLS protects SWIM.
3. Admission epochs fence reused certificate node IDs.
4. Every client request has a principal and authorization decision.
5. Rotation, revocation, expiry, partition failback, restart, and recovery pass
   live-traffic tests.
6. Unauthenticated work and audit volume remain bounded.
7. Bootstrap and credential-lifecycle runbooks exist.

Until then, EastGuard remains trusted-network-only.

---

## Security Invariants

1. **Every secure connection has one identity.** A client connection has one
   principal. A node connection has one certificate node ID, admission epoch,
   and `NodeId`. This prevents ambiguous authority.

2. **Every certificate node ID has one current admission.** The security state
   contains one highest epoch and `NodeId` for each certificate node ID. This
   fences older processes after restart or replacement.

3. **Every producer session has one principal owner.** This prevents cross-tenant
   sequence poisoning.

4. **Every cached security entry identifies its source and lifetime.** It contains
   one security key, version, and deadline. This makes stale state detectable.

5. **Audit state stays within configured capacity.** Detail queues and aggregate
   counters never exceed their fixed bounds.

### Security Rules

1. Secure mode never downgrades.
2. Transport authenticates identities; applications authorize operations.
3. Identity mismatch closes the connection before payload dispatch.
4. Validated transport identity accompanies every Raft and data-plane envelope.
5. Application denial drops one envelope, not the shared connection.
6. Within one admission epoch, higher SWIM incarnation wins.
7. A higher admission epoch always fences every older process.
8. Admission controls eligibility; SWIM controls liveness.
9. SWIM facts are processed only while their `NodeId` and epoch match a current
   admission in the local cache.
10. The data plane authorizes senders from committed local placement.
11. The executing broker authorizes every client operation.
12. Membership and administrative operations fail closed without fresh security
   state.
13. Established traffic fails closed when cached security state expires.
14. Resource permits are acquired before allocation or task creation.
15. Audit backpressure never blocks protocol processing.
16. Revoked credentials stop working within the enforcement deadline.
