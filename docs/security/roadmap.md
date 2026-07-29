# EastGuard Security Roadmap

**Goal:** Provide a secure, default-deny production boundary for all nodes and clients without sacrificing online certificate rotation, node restarts, or network partition recovery.

**Depends on:** SWIM membership, Raft consensus, data-plane placement, client protocol, and client redirect handling.

---

## 1. Overview & Threat Model

EastGuard operates in two distinct security modes:

- **Secure Mode (Default):** Mutual TLS 1.3 protects TCP listeners. Secure SWIM UDP is intentionally deferred until EastGuard can adopt a suitable datagram security implementation. Until then, secure mode fails startup rather than exposing plaintext SWIM. Unauthenticated or unauthorized traffic is immediately rejected. Plaintext connections and protocol downgrades are forbidden; invalid configuration prevents startup.
- **Trusted Development Mode:** Plaintext protocols enabled strictly via explicit opt-in configuration for isolated test environments.

### Threat Model
The production boundary defends against external network attackers attempting eavesdropping, packet modification, replay, UDP spoofing, connection injection, or TLS downgrade.

> **Scope Note:** Compromised brokers or authorized clients are outside this threat model. Their impact is constrained by short-lived credentials, scoped ACLs, and strict principal binding. Byzantine consensus is out of scope.

### Listener Architecture

| Listener | Port | Protocol | Peer Authentication | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Client** | TCP 2921 | TLS 1.3 | Mutual X.509 | Metadata queries, administration, produce, fetch |
| **Raft** | TCP 2922 | TLS 1.3 | Mutual X.509 | Metadata shard consensus log replication |
| **Data** | TCP 2923 | TLS 1.3 | Mutual X.509 | Segment replication, repair, and coordination |
| **SWIM** | UDP 2922 | Secure datagrams (deferred) | Mutual X.509 | Membership gossip and failure detection |

---

## 2. Layered Architecture

Security checks are split between the transport layer and application state machines to keep state machines free of security I/O:

```
       [ Authenticated Transport Layer ]
       TCP: TLS 1.3
       UDP: secure datagrams (deferred)
  - Authenticate peer X.509 certificates
  - Enforce framing, datagram MTU, and resource limits
  - Bind connection envelope sender to verified identity
                     │
                     │ (Drop connection on transport failure)
                     ▼
        [ Application Layer State Machines ]
  - Authorize requested operation against cached ACLs / placement
  - Execute SWIM / Raft / Data-Plane state transitions
                     │
                     │ (Drop denied envelope only; connection stays open)
```

- **Transport Responsibility:** Performs cryptographic handshakes, validates certificates, tracks cache expiry, and binds envelope senders to verified identities. Transport errors close the connection.
- **Application Responsibility:** Synchronous state machines (SWIM, Raft, Topology, Data Plane) process only pre-validated envelopes. An authorization failure drops the specific denied envelope without tearing down the underlying connection.
- **Placement-Based Data-Plane Authorization:** Data-plane nodes authorize incoming replication and repair requests against local committed placement state, ignoring sender-asserted replica lists.
- **Redirects:** A redirect is only an address hint. The destination authenticates the peer and repeats the authorization check.

---

## 3. Node Identity & Admission Model

To allow safe node restarts and hardware replacement without exposing the cluster to stale process replay, EastGuard decouples reusable identity from running process instances.

### Identity Hierarchy

| Term | Scope | Lifetime / Ordering | Function |
| :--- | :--- | :--- | :--- |
| **Node Certificate Principal** | X.509 Certificate | Long-lived / Reused | Operator-assigned node principal read from the authenticated certificate and used as the stable admission-record key. |
| **Admission Epoch** | Node Certificate Principal | Monotonically increasing `u64` | Assigned by metadata Raft upon restart; higher epoch **fences** older instances. |
| **NodeId** | Running Process | Single process lifetime | Unique ID generated on startup; used by SWIM, topology ring, Raft, and data placement. |
| **Process Key** | Running Process | Single process lifetime | Proves that the connection belongs to the process admitted for this epoch. |
| **SWIM Incarnation** | Running Process | Monotonically increasing counter | Incremented by the *same* process instance to refute false `Suspect`/`Dead` gossip. |

The Node Certificate Principal is the value after `urn:eastguard:node:` in one
URI Subject Alternative Name. A node certificate must contain exactly one such
URI; missing or repeated values fail authentication.

### Resolution Rules

Node identity conflicts and stale gossip are resolved in this order:

```
 1. Admission Epoch   ──► Higher epoch always wins & fences older processes
          │ (If equal)
          ▼
 2. SWIM Incarnation  ──► Higher incarnation refutes stale liveness facts
          │ (If equal)
          ▼
 3. Liveness State    ──► Dead > Suspect > Alive
```

### Partition Recovery vs. Node Restart

An **authorized operator** is a person or trusted automation using a client
certificate whose principal has the `security/cluster` grant. In an orchestrated
deployment, automation may approve restarts and scaling, but metadata Raft still
commits every admission.

- **Healed Partition (Same Process):** Retains its existing `NodeId` and `Admission Epoch`. Increments its `SWIM Incarnation` counter to refute `Suspect` or `Dead` rumors spread during the partition.
- **Node Restart / Replacement:** The process generates a new `NodeId` and process key. An authorized operator approves that exact process, and metadata Raft commits the new `Admission Epoch`, `NodeId`, and process public key. The higher epoch fences older processes immediately where observed and everywhere else within 60 seconds.

### Admission Gate & SWIM Separation

SWIM liveness gossip is decoupled from cluster admission authority to prevent network partitions from admitting unauthorized nodes:

```
  [ Authorized Operator ] ──► Approve Joining Node's NodeId & Process Key
                                             │
                                             ▼
  [ Joining Node ] ──► Limited Admission Endpoint ──► Metadata Raft
                                                        │
                                                        ▼
                                  [ Epoch + NodeId + Process Public Key ]
                                                        │
                                                        ▼
  [ SWIM Actor ] ◄──────────── Check Admission Cache ───┘
           │
           ▼ (Accepted)
  [ SWIM State Machine ] ──► Liveness Gossip (Alive / Suspect / Dead)
           │
           ▼
  [ Raft Reconciliation ] ──► Commit AddPeer / RemovePeer
```

1. **Process Proof:** A node session proves possession of the process private key bound to its admitted epoch. The process key signs the admitted identity and a session-specific value produced by the authenticated transport, so the proof cannot be replayed in another session. The reusable node certificate alone cannot create or claim a newer epoch.
2. **Admission Gate:** The SWIM actor checks incoming packets against a local admission cache backed by metadata Raft before passing facts to the SWIM state machine.
3. **Gossip Rule:** The secure datagram session authenticates the immediate sender. Every relayed membership fact is separately accepted only when its subject `NodeId` and `Admission Epoch` match an active admission record.
4. **Cache Policy:** Admission records are cached locally with a maximum TTL of 60 seconds. If a cache entry expires while the owning metadata shard is unreachable, the gate fails closed.

---

## 4. Authorization & Sharded Security Records

EastGuard enforces exact, default-deny access control lists (ACLs) without wildcards or inheritance.
A principal is the client ID read from an authenticated certificate and used for
permission checks; its text grants no authority by itself.

### ACL Resource Catalog

| Resource Key Format | Granted Actions |
| :--- | :--- |
| `cluster` | Create and list topics, membership inspection, topology lookup, operator diagnostics |
| `topic-admin/{topic-id}` | Delete and describe topic metadata |
| `topic-data/{topic-id}` | Produce, fetch, list offsets for topic |
| `consumer-group/{topic-id}/{group-id}` | Coordinate the group and read/commit its offsets |
| `producer-session/{topic-id}/{producer-id}` | Renew the session, bound to its creator for the session lifetime |
| `security/cluster` | Read/write ACLs, manage admissions and revocations, inspect security audit |

Consumer-group access permits group coordination and offset read/commit. Reading
records separately requires `Fetch` on `topic-data/{topic-id}`. Text resource
keys are used only at routing and administrative boundaries; replicated records
store the typed resource directly.

### Sharded Metadata Storage

A security record is one durable admission, ACL, or revocation entry. Its record
path selects one metadata shard; its revision lets brokers detect stale cached
copies.

Security records (`security/node/{node-certificate-principal}`, `security/acl/{resource}`, `security/revocation/{issuer}/{serial}`) do not rely on a centralized security controller. Instead, they hash to standard metadata shards and replicate via Raft:

```
  Client/Node Request ──► Any Broker ──► Hash Record Path ──► Hosts Shard? ─┬─► Yes ──► Commit via Raft
                                                                           └─► No  ──► Return Owner Redirect
```

- **Stable Admission Key:** A restart changes the process `NodeId` and key, but
  not the Node Certificate Principal. The same record and metadata shard therefore
  replace the old admitted process atomically:

```
security/node/{node-certificate-principal}
                 │
                 └── Admission Epoch + NodeId + Process Public Key
```

- **Local Authorization:** Brokers evaluate ACLs against local cached security records.
  Data permissions use the stable topic ID, so a data replica can authorize a
  request without hosting that topic's metadata shard.
- **Freshness & Expiry:** Cached records include a monotonic deadline (max 60s)
  and revision counter. An expired entry cannot authorize a request. The chosen
  distribution mechanism must obtain current state or fail closed.

---

## 5. Operations & Credential Lifecycle

### Bootstrap & Node Joining

1. Operator initializes a cluster trust root and issues the first node certificate.
2. The first node generates its `NodeId` and process key. Initial metadata state
   stores that admission, the first operator principal, and its
   `security/cluster` grant.
3. A later joining node generates a new `NodeId` and process key.
4. An authorized operator approves that exact `NodeId` and process public key. The reusable node certificate alone cannot authorize replacement.
5. The joining node connects to a **limited admission endpoint** using its X.509 certificate.
6. The endpoint uses the authenticated Node Certificate Principal to route to its
   admission record. The owning metadata shard atomically replaces the prior
   process with the next `Admission Epoch`, `NodeId`, and process public key.
7. The joining node proves possession of the process private key before entering SWIM gossip and Raft membership reconciliation.

### Online Credential Rotation

- **Zero-Downtime CA Rotation:** Brokers support dual trust chain loading. New root CAs can be added and leaf certificates reloaded online without restarting brokers or changing `Admission Epoch` / `NodeId`.
- **Revocation & Expiry:** Certificate revocations commit to metadata Raft records. Active authenticated sessions are terminated within the cache enforcement window. Expired certificates are rejected with clock-skew tolerance.
- **Recovery:** Runbooks cover lost authorized-operator access, lost issuing keys, expiry, accidental revocation, trust-root replacement, and cold-cluster restart.

---

## 6. Resource Limits & Security Audit

### Rate & Memory Bounds
- Every listener enforces strict limits on unauthenticated handshakes, concurrent connections, in-flight frames, and memory allocations.
- Client request-rate limits are deferred. The design must first define whether
  replicas of one application share a principal, which limits are node-wide, and
  how limits behave as a Kubernetes workload scales.
- The future secure UDP transport must define a payload budget that avoids IP fragmentation after authentication and encryption overhead.

### Client Request Boundary

Clients normally route directly to the data replica named by their current topic
metadata. A redirect is only recovery from stale routing; brokers never proxy a
produce or fetch to another data node.

```
Client request
      │
      ▼
Authenticate certificate
      │
      ▼
Check local ACL cache
      ├── Current grant
      ├── Current denial ─► Return unauthorized
      └── Missing / expired
                   │
                   ▼
       Obtain current ACL state
       (distribution design deferred)
                   │
             ┌─────┴─────┐
             ▼           ▼
           Grant      Deny / unavailable ──► Fail closed
             │
             ▼
Does this node serve the requested data?
      ├── No  ──► Return data-node redirect
      └── Yes ──► Execute locally
```

Authorization precedes redirects so an ungranted client cannot use stale-route
responses to discover data placement. Any remote authorization request may
update only the ACL cache; it must never carry or execute the client's data
operation.

The ACL distribution mechanism is intentionally deferred. A later phase may use
lazy pull, proactive push, or a push-and-pull hybrid:

| Model | Benefit | Failure to handle |
| :--- | :--- | :--- |
| Pull on cache miss or expiry | Simple; clients that leave create no update traffic | A data node must fail closed if the owner is unavailable |
| Push changed records | Fast local decisions | A disconnected data node can miss an update |
| Push plus periodic pull | Fast normally; repairs missed updates | More protocol and cache synchronization logic |

The same decision gate covers request-rate limiting. A Kubernetes deployment may
give all replicas of one application a shared principal even though each replica
has a different leaf certificate. A fixed per-principal table size or request
budget is therefore premature: one shared principal can represent many clients,
while one principal per replica can grow with autoscaling. No client request-rate
limit is implemented until identity granularity, node-wide capacity bounds, and
cache distribution are chosen together.

### Secure UDP Decision

EastGuard retains UDP for SWIM because connection-oriented transport does not
fit membership at cluster scale:

```
       SWIM probes one peer per interval
                    │
                    ▼
              stateless UDP
                    │
       ┌────────────┴────────────┐
       ▼                         ▼
constant socket count      packet loss remains
per node                   visible to SWIM
```

TCP with bounded connection reuse was rejected because large clusters would
trade an unbounded connection mesh for continuous handshakes, connection churn,
kernel connection tracking, and head-of-line blocking. QUIC datagrams preserve
UDP delivery semantics, but their per-peer connection state and implementation
complexity are not justified for SWIM's sparse traffic. Current DTLS options do
not meet the combined requirements for maturity, permissive licensing, Rust
integration, and deterministic simulation.

Secure SWIM therefore remains deferred. Trusted development mode may use
plaintext UDP in isolated environments. Secure mode must fail startup until a
secure datagram transport is selected and implemented; it must never fall back
to plaintext UDP.

### Acceptance Criteria for a Future Secure UDP Transport

The selected transport must:

| Requirement | Reason |
| :--- | :--- |
| Preserve datagram boundaries and loss | SWIM timeouts and indirect probes must observe loss rather than transport retransmission delays |
| Keep per-node transport state bounded independently of cluster size | Membership must remain viable for clusters with thousands of nodes |
| Authenticate node certificates and expose the certificate principal | Admission must bind each packet source to a verified node identity |
| Reject replay and spoofed source traffic | Old or forged membership packets must not alter liveness |
| Avoid IP fragmentation | One lost fragment must not discard an oversized protected packet |
| Run over EastGuard's UDP abstraction | Production and turmoil must exercise the same protocol state machine |
| Use virtual time in deterministic tests | Handshake retry, expiry, and packet loss must be reproducible |
| Use a mature, maintainable, permissively licensed dependency | Cluster security must not rely on an unaudited or incompatible implementation |

### Audit Subsystem
- **Non-Blocking Execution:** Security audit events (authentication success/failure, ACL denials, admissions) are queued asynchronously. Audit backpressure never blocks protocol execution or consensus.
- **Rate-Limited Flood Protection:** High-frequency audit events use aggregate counters and sampled detail logging.
- **Credential Hygiene:** Audit logs never record private keys, tokens, credentials, or message payloads.

---

## 7. Delivery Plan (S0–S6)

```
S0 ──► S1 ──► S2 ──► S3 ──► S4 ──► S5 ──► S6
config  records  TCP     SWIM    clients  operations  production
                 mTLS  deferred  + ACLs   + audit     gate
```

| Phase | Target Scope | Key Deliverable | Exit Criteria |
| :--- | :--- | :--- | :--- |
| **S0** | Configuration | Security modes and certificate loader | Secure mode opens no plaintext listeners and fails startup while any required secure listener is unavailable |
| **S1** | Metadata Storage | Security record schema, sharded Raft state | Security records survive snapshot & recovery |
| **S2** | Cluster Transport | TLS 1.3 on TCP 2922/2923, Raft sender and role authorization | Authenticated and authorized cluster TCP traffic |
| **S3** | Membership (deferred) | Secure datagrams on UDP 2922 and SWIM admission gate | A transport meeting the secure UDP acceptance criteria provides authenticated gossip and partition-safe admission fencing |
| **S4** | Client API | Client mTLS on TCP 2921, principal binding, ACLs | Default-deny enforcement on all client APIs |
| **S5** | Operations | Certificate rotation, revocation, expiry, recovery, audit logging | Online credential operations and recovery runbooks |
| **S6** | Production Gate | Adversarial testing, fuzzing, partition stress | Passes all production readiness checks |

S6 must verify node and client impersonation, stale-process replay, unauthorized
operations, protocol downgrade, rotation under live traffic, expired and revoked
credentials, cold-cluster restart, handshake and datagram fuzzing, resource
bounds, secret-free diagnostics, and reproducible secure-SWIM behavior under
turmoil with pinned randomness and node identities. S6 cannot pass while S3 is
deferred.

---

## 8. Invariants & Security Rules

### System Invariants

1. **Single Connection Identity:** Every established client connection has exactly one authenticated principal; every node connection has exactly one `(Node Certificate Principal, Admission Epoch, NodeId, Process Public Key)`.
2. **Unique Active Node Admission:** Metadata state maintains at most one active `(Admission Epoch, NodeId, Process Public Key)` per Node Certificate Principal.
3. **Immutable Producer Session Principal:** Every producer session is immutably bound to the principal that created it.
4. **Explicit Cache Bounding:** Every cached security entry specifies its source metadata shard, revision, and expiry measured with a monotonic clock (≤ 60 seconds).
5. **Bounded Audit Footprint:** Audit queues and aggregate rate counters remain within configured capacity.

### Operational Rules

1. Secure mode never downgrades to plaintext or unauthenticated protocols.
2. Transport layers authenticate identity; application state machines authorize actions.
3. Transport identity mismatches close the connection immediately before payload dispatch.
4. Application authorization denials drop only the denied envelope, preserving the connection for valid traffic.
5. A higher admission epoch fences every older process immediately where observed and no later than admission-cache expiry elsewhere.
6. SWIM controls liveness; metadata Raft controls admission.
7. Missing or expired security records cause authorization and admission checks to fail closed.
8. Resource permits are acquired before allocating memory or spawning async tasks.
9. Audit logging backpressure must never block network protocol processing or consensus.
10. Only an authorized operator may approve a higher admission epoch.
