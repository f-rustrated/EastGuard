# EastGuard Security Roadmap

**Goal:** Keep authentication integration, minimal broker permissions, and
protocol hardening. Delegate credential infrastructure and network protection.

## Deployment scope

Trusted brokers run inside an operator-protected network covering TCP **and SWIM
UDP**. Standard mTLS supplies verified principals; EastGuard checks their static
permissions. Policy and credential changes use deployment/restart procedures.
Compromised brokers are outside this trust model.

The [deployment contract](deployment.md) defines certificates, policy format,
credential replacement, and closing existing sessions when access is withdrawn.

## Implemented

- Bounded startup policy, with exact topic-data, topic-admin, consumer-group,
  and cluster grants. Missing identity or permission denies access.
- Authorization before routing/redirects; topic name/ID binding; producer-owner
  checks. Data clients can discover routes without deletion permission.
- SDK mTLS across seeds, redirects, and reconnects, including destination checks.
- Distributed ACL actors, quorum reads, caches, expiry bookkeeping, persisted
  security records, and grant/revoke commands removed. No legacy compatibility:
  this project has never been deployed.
- Bounded record counts, decompression, fetch assembly, and wire frames;
  payload-free error logs, handler limits, and transport timeouts retained.

## Remaining before secure startup

1. Enforce committed data placement at receiving boundaries. Raft membership
   and authenticated identity must not substitute for segment replica authority.
2. Verify cluster formation, restart, and partition recovery with mTLS and the
   protected-network contract. Then remove the secure-startup guard.

Secure startup still aborts before listeners open. Trusted development remains
explicit. Keep production-code and test-code reductions visible when reporting
changes; simplification must remove the replaced implementation and its tests.

## Deferred until required

Dynamic persisted ACL administration, online reload, broker-managed revocation,
fixed session-closure deadlines, a separate audit pipeline, and EastGuard-owned
secure datagrams. These are deployment-specific extensions, not prerequisites.
