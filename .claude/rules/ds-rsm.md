# DS-RSM (System-Level Invariants)

Top-level architecture for EastGuard's metadata management. Sharded Raft groups across the cluster, SWIM for membership. For the full conceptual picture, see `diagrams/metadata-management/mental-model.md`.

## Invariants

1. **Each Raft instance is fully independent.** A node runs many Raft instances (one per shard group it hosts). They share no state — separate term, voted_for, log, commit_index, peers. `MultiRaftActor` maps `ShardGroupId → Raft`. Processing event for one shard never touches another's state.

2. **SWIM is sole membership authority.** Group lifecycle (create, remove) and peer changes (add, remove) are driven by SWIM membership events. `MultiRaftActor` does not decide when to create or remove groups — only responds to commands from `SwimActor`.

3. **Raft is transport-agnostic.** `Raft::peers` is `HashSet<NodeId>` — no addresses. State machine produces `OutboundRaftPacket { target: NodeId, rpc }`. Actor/transport layer resolves `NodeId → SocketAddr → Connection`.

4. **Sync-first design.** All protocol logic (`Swim`, `Raft`, `MetadataStateMachine`, `Ticker`, `GossipBuffer`, `Topology`) is purely synchronous. No async, no I/O, no channels. Side effects buffered internally and drained by owning actor after each event.

5. **Every event must be followed by flushing output buffers.** After every `step()`, `handle_timeout()`, or `propose()` call, the actor must drain `take_outbound()`, `take_timer_commands()`, `take_events()`, and `take_log_mutations()`. Skip flush → packets and timer commands silently lost.

6. **Timer seq values unique across all shard groups on a node.** `MultiRaft` uses `RaftTimerTokenGenerator` (monotonic wrapping counter) to assign globally unique timer seq values. Prevents cross-group timer collisions in shared `Ticker`.

7. **Leader forwarding is max 1 hop.** `ProposeRequest.forwarded: bool` prevents re-forwarding. Non-leader forwards to leader once; if that also fails, error returned to client.

8. **Stale proposals are safe.** Split/merge/seal proposals that arrive after state has changed are rejected by `apply_*` precondition checks in `MetadataStateMachine`. Two leaders briefly coexisting (network partition) cannot cause inconsistency — only one proposal commits.

9. **No cross-shard coordination for single-topic operations.** Topic and all its ranges and segments live in one shard group. Every mutation is a single Raft log entry — atomic by construction. Cross-topic operations (e.g., list all topics) require scatter-gather.

10. **SWIM facts that no node could convert stay outside Raft until a leader exists.** SWIM does not re-fire already-disseminated events. A leader that takes over after such a gap must explicitly refer back to SWIM and propose the missed conversions. See `raft-actor.md` invariant on leader-election reconciliation.

11. **Membership changes flow through the Raft log on every replica, never via direct mutation.** Direct mutation from gossip events would let replicas disagree on quorum size at the same log index, admitting split-brain commit. See `raft.md`.
