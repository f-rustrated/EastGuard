# SWIM (Invariants)

`Swim` is the synchronous SWIM failure-detection state machine. `SwimActor` is the async wrapper that drives it. For the protocol's role in metadata membership, see `docs/metadata-management/d4_membership_and_shard_reconciliation.md`.

## Architecture (brief)

```
mpsc::Receiver<SwimCommand>
            ↓
       SwimActor   (mailbox)
            ↓
        Swim   (sync state machine; produces SwimEvents)
            ↓
   take_events() → routed to:
       Packet     → transport_tx
       Timer      → scheduler_tx
       Membership → raft_tx (HandleNodeDeath / HandleNodeJoin)
```

`SwimCommand` variants: `InboundRaftRpc` (from UDP transport), `Timeout` (from ticker), `Query` (from external callers).

## Invariants

1. **Self-incarnation monotonicity.** A node's own published incarnation number only increases. A running node bumps its incarnation when it refutes suspicion or stale death gossip after a healed partition, never resets. Without this, false-positive suspicions or a healed partition could permanently shadow a healthy node.

2. **Incarnation-then-state ordering decides conflicts.** Comparing two messages about node X: the one with the higher incarnation wins. For equal incarnations, Dead overrides Suspect overrides Alive. This single rule prevents gossip oscillation ("zombie loops") where contradicting state updates ping-pong forever.

3. **Suspect → Dead requires timeout, not gossip alone.** A node enters Dead only after the Suspect timer expires without refutation. Receiving gossip claiming "X is Suspect" never promotes X directly to Dead — the receiver runs its own timer.

4. **Probe-ack correlation by sequence number.** Each probe carries a unique `seq`; acks must echo the same `seq` to cancel the corresponding timer. Acks without a matching outstanding seq are ignored. Prevents stale or spoofed acks from acknowledging in-flight probes.

5. **Suspect does not trigger Raft-group-lifecycle changes.** Only `Alive` and `Dead` events translate to `HandleNodeJoin` / `HandleNodeDeath`. Suspect would cause churn on every flap; correctness requires waiting for confirmed Dead before mutating any group's intent.
