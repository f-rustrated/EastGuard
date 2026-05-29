# Topology (Invariants)

`Topology` — consistent hash ring mapping resource keys to shard groups, plus a tracker of current Raft leaders per shard group. Maintained locally on every node, updated via SWIM membership events. Pure synchronous data structure. For the conceptual picture, see `diagrams/metadata-management/mental-model.md` § "Topology — the hash ring".

## Structure

```
Topology
├── vnodes: BTreeSet<VirtualNodeToken>           (hash ring positions)
├── groups: HashMap<ShardGroupId, ShardGroup>    (canonical shard group definitions)
├── node_group_ids: HashMap<NodeId, Vec<ShardGroupId>>  (reverse index)
└── shard_leaders: HashMap<ShardGroupId, ShardLeaderEntry>  (leader map)
```

`VirtualNodeToken` sorted by `(hash, pnode_id, replica_index)` — deterministic tiebreaker on hash collision.

## Invariants

1. **Hash routing is deterministic.** Same key + same ring state → same shard group, on every node. Required for cluster-wide routing convergence without a central coordinator.

2. **Each topic name maps to exactly one shard group.** `hash(topic_name)` deterministically routes to one group; within that group, `topic_name_index` rejects duplicates. Two different shard groups can never own the same topic name. This is what makes "topic name" a globally unique key without coordination.

3. **A shard group never lists the same physical node twice.** Walking the ring skips duplicate `pnode_id`s when selecting a group's members. A group with duplicate members would have inflated apparent replication while actually holding fewer than `replication_factor` copies.

4. **Reverse index is consistent with the vnode set.** `node_group_ids` always reflects exactly the groups derived from the current `vnodes`. Ring mutations rebuild the index from scratch rather than patching incrementally — eliminates the class of bugs where partial updates leave stale entries.

5. **Insert is idempotent.** Adding a node already on the ring is a no-op. Re-receiving an `Alive` event during gossip convergence does not corrupt the ring.

6. **Shard leader map is term-monotonic.** An update is accepted only if its term is strictly higher than the existing entry. Equal or lower terms rejected. Prevents a deposed leader from re-installing itself via stale gossip.

7. **Leader map entries persist until overwritten.** A node's death does not clear its leader entries; they're only replaced by a higher-term claim from a re-election. Avoids a window where "no leader is known" — clients would fail to route during the gap.

8. **Suspect does not mutate the ring.** Only `Alive` (insert) and `Dead` (remove) change topology. Suspect leaves the node in place — the node may refute. Otherwise topology would churn on every flap.
