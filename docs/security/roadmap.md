# EastGuard Security Roadmap

**Goal:** Make secure deployment the default without preventing online rotation,
node restart, or recovery from a network partition.

EastGuard will provide:

1. Cryptographic identity for every node and client.
2. TLS 1.3 for TCP and DTLS 1.3 for SWIM UDP.
3. Default-deny authorization on every broker.
4. Safe node admission, stable-ID reuse, rotation, revocation, and audit.

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

Transport closes a connection for authentication, framing, or envelope-identity
failure. An application authorization failure drops only that envelope, so valid
traffic for other groups and segments continues on the shared connection.

Raft authorization follows
[D8 — Raft RPC Sender Authorization](../metadata-management/d8_raft_rpc_sender_authorization.md).
Metadata Raft remains the placement authority. Each data-plane node caches only
the committed placement needed for its local segments and uses that cache—not a
replica list supplied by an incoming message—to authorize replication, repair,
and coordination.

Clients authenticate every redirect destination as a broker in the same cluster.
The destination authenticates and authorizes the client again; a redirect grants
no authority.

---

## Identity

### Node lifecycle identity

A node has four lifecycle values:

| Level | Meaning |
|---|---|
| **Stable node ID** | Operator-visible identity reused across partitions and restarts |
| **Admission epoch** | Monotonic generation authorizing one lifecycle of that stable ID |
| **Boot identity** | Fresh key pair for one process lifecycle |
| **SWIM incarnation** | Monotonic conflict counter within that boot |

Three similar connection terms have different jobs:

| Term | Source | Purpose |
|---|---|---|
| **Certificate node ID** | Stable node ID inside the X.509 certificate | Proves which logical node owns the credential |
| **Boot identity** | Fresh public key generated when the process starts | Distinguishes this process from older processes using the same stable node ID |
| **Handshake identity** | Stable node ID, admission epoch, and boot identity presented on a connection | Binds this connection to the currently admitted process |

The handshake identity is not a third generated ID. It is the connection claim
that joins the certificate node ID to the admitted boot identity:

```
certificate node ID = A
boot identity       = Y
handshake identity  = (A, epoch 8, Y)
```

At startup, the process creates a boot key pair. The metadata shard owning its
security record commits:

```
stable node ID + admission epoch + boot public key + certificate fingerprint
```

Every new boot receives an epoch greater than the previous epoch for that stable
node ID. Only the boot identity in the latest committed epoch may create new
cluster sessions.

**Connection identity rule:** The certificate proves the stable node ID. The
admission record proves the epoch and boot identity. The handshake must present
that exact tuple. A peer accepts a new node connection only after verifying the
current record through its cache or the owning metadata shard.

### Partition failback and restart

Stable node-ID reuse is supported in two forms:

- **Healed partition:** The process keeps its admission epoch and boot identity.
  It increases its SWIM incarnation to refute stale `Suspect` or `Dead` gossip.
- **Restart or replacement:** The process uses a new boot identity and receives a
  new admission epoch. The newer epoch fences every older boot, regardless of its
  SWIM incarnation.

Identity facts are ordered by admission epoch first and SWIM incarnation second:

```
higher admission epoch wins
same epoch -> higher incarnation wins
same incarnation -> Dead > Suspect > Alive
```

An old partitioned process cannot return after a replacement and reclaim its
stable node ID. Its epoch is stale, so its sessions and gossip are rejected.

### Signed SWIM facts

Each boot signs its own cluster ID, stable node ID, admission epoch, boot identity,
addresses, and incarnation. Relays forward this assertion unchanged.

An observer signs a `Suspect` or `Dead` report and includes the exact subject
assertion. A report about an older epoch cannot affect a newer boot.

**Gossip rule:** Accept a SWIM fact only when its boot signature is valid, its
admission epoch is current, and its ordering is newer than local state.

### Client identity

A client certificate maps to one immutable principal for the connection.
Request IDs, producer IDs, consumer-group member IDs, and topic names are
application data, not credentials.

A producer session is permanently bound to the principal that created it.
Requests from another principal are rejected before sequence, fencing, expiry, or
deduplication state changes.

---

## Authorization

Secure mode uses default-deny ACLs. Explicit denies override grants.

| Resource | Actions |
|---|---|
| Cluster | Inspect membership and shards; operator/debug actions |
| Topics | Create, delete, describe, list |
| Topic data | Produce, fetch, list offsets |
| Consumer groups | Consume; inspect or reset progress |
| Producer sessions | Open, renew |
| Security | Read/change ACLs, revoke identities, inspect audit |

`Consume` covers join, heartbeat, leave, assignment, and offset read/commit.
Reading records also requires `Fetch` on the topic. Offset inspection or reset is
a separate administrative grant.

### Sharded security records

EastGuard has no dedicated security group or controller. Security records use the
existing sharded metadata system:

| Key | Value |
|---|---|
| `security/node/{stable-id}` | Current admission epoch and boot identity |
| `security/acl/{resource}` | ACL |
| `security/revocation/{serial}` | Credential revocation |

Each key hashes to an ordinary metadata shard and commits through that shard's
Raft log. Any broker can receive a request and route it to the owning shard.

```
security request
       |
       v
any broker
       |
       v
hash security key
       |
       v
ordinary metadata shard -> Raft commit
```

Brokers authorize locally from versioned, deadline-bound snapshots of these
records. The broker executing an operation always repeats the check, including
after redirects and retries.

---

## Bootstrap and Join

Bootstrap starts from operator-provided trust material:

1. Create the cluster identity and trust root.
2. Issue the first node certificate.
3. Start the first node in secure mode.
4. Authenticate a candidate's certificate and boot key.
5. Route `security/node/{stable-id}` to its metadata shard.
6. Atomically commit the next admission epoch and boot identity.
7. Allow that boot to enter SWIM and Raft membership flows.

Knowing a seed address grants no authority. A candidate cannot change SWIM,
topology, or Raft state before admission commits.

Reusing a stable node ID is a new admission, not an implicit resurrection. The new
epoch must commit before the replacement joins. The restarted process does not
choose an epoch: the owning metadata shard reads the current record and commits
the next value. If that shard lacks quorum, admission waits.

---

## Credential Lifecycle

### Rotation

1. Add the new trust chain.
2. Reload trust while the old chain remains valid.
3. Issue and reload new leaf certificates.
4. Reconnect long-lived sessions.
5. Remove the old chain and verify rejection.

Nodes reload certificates and trust without restarting. Rotation does not require
a cluster-wide outage.

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
Cached policy is never used after its signed deadline.

Operations documentation covers lost keys, issuing-key loss, accidental
revocation, expiry, loss of quorum in a metadata shard that owns security records,
trust-root replacement, and stable-node-ID recovery. Recovery never enables
plaintext.

---

## Resource Limits and Audit

Every listener bounds connections, unauthenticated handshakes, attempts per
source, deadlines, certificate and frame sizes, in-flight requests, tasks,
buffers, and event rates. Invalid DTLS packets are dropped before SWIM decoding.

Every encrypted SWIM packet fits in one UDP datagram. The DTLS record carried as
the UDP payload is limited to 1200 bytes by default, leaving room for IPv6 and UDP
headers within the 1280-byte minimum IPv6 MTU. The gossip budget is what remains
after DTLS and the fixed SWIM envelope. Oversized outbound packets are rejected
before sending; oversized inbound packets are dropped before decoding.

Security audit events record the reporting node, normalized principal, endpoint,
cluster, operation, resource, result, stable reason code, correlation ID, and a
non-secret certificate fingerprint when useful.

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
| **S0** | Modes, configuration, certificate loading | Secure mode opens no plaintext listener |
| **S1** | Cluster TCP mTLS, envelope binding, Raft D8, data-plane authority | Authenticated cluster traffic |
| **S2** | DTLS, signed SWIM facts, admission epochs and fencing | Safe membership and stable-ID reuse |
| **S3** | Client mTLS and principal propagation | Every request has one principal |
| **S4** | Sharded security records, ACL snapshots, enforcement | Every operation is default-deny |
| **S5** | Reload, rotation, expiry, revocation, recovery, audit | Online credential operations |
| **S6** | Adversarial, fuzz, partition, and load tests | Production readiness gate passes |

Dependencies: S0 precedes S1 and S3; S1 precedes S2; S3 precedes S4; S2 and S4
precede S5; S5 precedes S6.

---

## Validation

### Identity and transport

- Use credentials for another cluster or node.
- Mismatch stable node, epoch, boot, handshake, or envelope identities.
- Replay an older admission epoch after a replacement joins.
- Heal a partition and verify the same boot refutes stale death with a higher
  incarnation.
- Start a replacement and verify its higher epoch fences the old partitioned boot.
- Modify or replay signed SWIM assertions and observer reports.
- Attempt plaintext and protocol downgrade on every listener.
- Verify encrypted SWIM UDP payloads stay within 1200 bytes and oversized inbound
  or outbound packets are dropped without IP fragmentation.
- Fuzz handshakes, datagrams, routing envelopes, and frame decoders.

### Application authority

- Run the Raft D8 authorization suite.
- Send data-plane messages from outside committed placement.
- Verify one rejected group or segment does not disrupt others on the connection.
- Test every client action with no grant, unrelated grant, exact grant, wildcard
  grant, and explicit deny.
- Use one principal's producer session from another principal.
- Verify redirects and retries repeat authorization.

### Operations and load

- Rotate and revoke credentials during elections, produce, fetch, replication,
  and repair.
- Partition brokers from security-record shards before and after snapshot expiry.
- Test expiry, clock skew, recovery, handshake floods, audit floods, and resource
  bounds.
- Verify diagnostics disclose no credential material.

---

## Production Readiness Gate

Production deployment requires:

1. Secure mode is the default and cannot downgrade.
2. TLS protects TCP and DTLS protects SWIM.
3. Admission epochs fence reused stable node IDs.
4. Every client request has a principal and authorization decision.
5. Rotation, revocation, expiry, partition failback, restart, and recovery pass
   live-traffic tests.
6. Unauthenticated work and audit volume remain bounded.
7. Bootstrap and credential-lifecycle runbooks exist.

Until then, EastGuard remains trusted-network-only.

---

## Security Invariants

1. **Every secure connection has one identity.** A client connection has one
   principal. A node connection has one cluster, stable node ID, admission epoch,
   and boot identity. This prevents ambiguous authority.

2. **Every stable node ID has one current admission.** The security state contains
   one highest epoch and boot identity for each stable node ID. This fences older
   boots after restart or replacement.

3. **Every accepted SWIM assertion has one valid subject.** Its stable node ID,
   admission epoch, boot identity, addresses, and incarnation have a valid boot
   signature and current admission. This prevents relays and old boots from
   rewriting identity.

4. **Every accepted SWIM failure report has one observer.** The observer signature
   is valid and references an unchanged subject assertion. This preserves
   transitive gossip without allowing a report about an old boot to kill a new
   one.

5. **Every producer session has one principal owner.** This prevents cross-tenant
   sequence poisoning.

6. **Every Raft envelope carries the transport-authenticated sender.** D8 performs
   group-level authorization without trusting a payload-only identity.

7. **Every dispatched data-plane envelope has an authorized sender.** Committed,
   versioned placement grants the required role.

8. **Every cached security snapshot is authenticated, versioned, and unexpired.**
   It comes from the shard that owns the security key. This bounds stale
   authorization and revocation.

9. **Audit state is bounded.** Detailed records and aggregate counters cannot
   exhaust protocol memory.

### Security Rules

1. Secure mode never downgrades.
2. Transport authenticates identities; applications authorize operations.
3. Identity mismatch closes the connection before payload dispatch.
4. Application denial drops one envelope, not the shared connection.
5. Within one admission epoch, higher SWIM incarnation wins.
6. A higher admission epoch always fences every older boot.
7. The executing broker authorizes every client operation.
8. Membership and administrative operations fail closed without fresh security
   state.
9. Established traffic fails closed when cached security state expires.
10. Unauthenticated work stays within configured limits.
11. Audit backpressure never blocks protocol processing.
12. Revoked credentials stop working within the enforcement deadline.
