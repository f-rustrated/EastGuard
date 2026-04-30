# Topology

## Purpose

`Topology` — consistent hash ring mapping resource keys to shard groups and tracking shard leader locations. Maintained locally on every node, updated via SWIM membership events. Pure synchronous data structure.

## Architecture

```
Topology
├── vnodes: BTreeSet<VirtualNodeToken>           (hash ring positions)
├── groups: HashMap<ShardGroupId, ShardGroup>    (canonical shard group definitions)
├── node_group_ids: HashMap<NodeId, Vec<ShardGroupId>>  (reverse index)
└── shard_leaders: HashMap<ShardGroupId, ShardLeaderEntry>  (leader map)
```

`VirtualNodeToken` sorted by `(hash, pnode_id, replica_index)` — deterministic tiebreaker on hash collision.

## Key Operations

| Operation | Trigger | Effect |
|---|---|---|
| `insert_node` | SWIM `Alive` event | Adds `vnodes_per_pnode` virtual nodes, rebuilds reverse index |
| `remove_node` | SWIM `Dead` event | Removes all virtual nodes for that pnode, rebuilds reverse index |
| `shard_group_for(key)` | Routing query | Walks ring clockwise from `hash(key)` to nearest vnode |
| `shard_groups_for_node(id)` | Node join/death handling | O(1) lookup via reverse index |
| `update_shard_leader(info)` | SWIM gossip piggybacking | Updates leader map if term is strictly higher |

## Invariants

1. **Deterministic hash routing.**  Same key always maps to same shard group given same ring state.

2. **Topic name globally unique via hash routing.** `hash(topic_name)` deterministically routes to exactly one shard group. Within that shard group, names are unique (enforced by `topic_name_index`). Two different shard groups can never both own the same topic name.

3. **Shard group members are distinct physical nodes.** `token_owners_at()` skips duplicate `pnode_id`s when walking the ring. A shard group never lists the same node twice.

4. **Ring mutation triggers full reverse index rebuild.** `insert_node()` and `remove_node()` always call `rebuild_reverse_index()`. `groups` and `node_group_ids` maps are rebuilt from scratch, never patched incrementally — prevents stale entries.

5. **Insert is idempotent.** Adding a node already on the ring is a no-op (`add_pnode` checks `node_group_ids` existence first).

6. **Shard leader map is term-monotonic.** `update_shard_leader()` accepts update only if `info.term > existing.term`. Equal or lower terms rejected. Prevents stale leader claims from deposed leaders.

7. **Shard leader map NOT cleared on node death.** Removing a node from the ring does not clear its leader entries. Stale entries are eventually overwritten by Raft re-election gossip with higher term. This avoids a gap where no leader is known during re-election.

8. **ShardGroupId derived from nearest vnode hash.** `ShardGroupId(vnode_token.hash as u64)`. Deterministic given ring state — no randomness, no counter.

9. **Suspect state is a no-op for topology.** Only `Alive` (insert) and `Dead` (remove) trigger ring mutations. Suspect does not modify the ring — the node remains in the ring until confirmed dead.
