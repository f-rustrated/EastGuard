# EastGuard Architecture (ZK-less)

Maintaining separate metadata store will introduce:
- complexity 
- maintainability issue

And this is not only when we develop the project but also for those who would potentially tab into the project. 

Assigning a dedicated controller role to some node(or group of) can be an option, and in fact that's a strategy taken by Kafka 4.0 

That has two issues: 
- Scalability issue by controller being a bottleneck
- Operability issue as external service is required to move existing data

So, In this discussion, I suggest the followings:
- `Scalable Weakly-consistent Infection-style Membership(SWIM)`
- `Dynamically-Sharded Replicated State Machine(DS-RSM)`



## SWIM
SWIM was created to solve the scaling limitations of traditional "heartbeat" protocols (where every node checks on every other node, which causes network congestion as the cluster grows). 

SWIM has two main components, and this is where the distinction lies:

1. Failure Detection (The "Probe")
This part is unique to SWIM and is not standard gossip.

- `Direct Ping` : Node A picks a random Node B and pings it.
- `Indirect Ping`: If Node B doesn't reply, Node A asks specific other nodes (C and D) to ping B on its behalf (this is called ping-req).
  - you can configure maximum number of peers for dissemination(for example, k=3)

This mechanism prevents false positives (marking a node dead just because the link between A and B is slow).

2. Dissemination (The standard "Gossip")
Once Node A confirms that Node B is dead (via the Failure Detection phase), it needs to tell everyone else.

Instead of broadcasting to the whole cluster "B IS DEAD!", Node A piggybacks this information onto its future messages to other nodes(e.g., when it probes).

This information spreads "infection-style" (gossip) through the cluster until everyone updates their membership list. 

- `Scenario`: Node A knows that Node C is dead.
- `Action`: When Node A sends its regular Ping to Node B, it adds a tiny footer to the message: "By the way, C is dead."
- `Cost`: This adds a few bytes to a packet that was going to be sent anyway. It does not create a new packet.


In essence, SWIM is to maintain the membership list in the cluster. 
We can consider handling the following event types:
- `Alive`(Join/Heartbeat)
  - *Trigger*: When a node boots up/refutes a suspicion
  - Payload: Node ID, Node's physical attributes (IP, Port, Rack), `Incarnation Number`(more on in the next subsection).
- `Suspect`(Failure detection) 
  - *Trigger*: A missed ping - this puts the node in probation
- `Confirm`(Dead)
  - *Trigger*: timeout after `Suspect` state. All nodes remove the node in question from their ring calculations.

And as "Piggybacking", we can introduce the following event types as well:
- `ShardLeader` (Metadata Routing)
  - Trigger: When a Raft election completes within a DS-RSM group.
  - Payload: ShardID, Leader Node ID, TermID.
  - Purpose: Tells the cluster (and proxies) exactly which node to contact for metadata regarding a specific hash range. The TermID is crucial here to ignore old claims from deposed leaders.


### Preventing "Zombie Loop" Using Incarnation Number
Without `Incarnation Numbers`, distributed failure detection is impossible to stabilize.

- `Node B` can’t reach `Node A`. `Node B` gossips: "`Node A` is Suspect!"
- `Node A` hears this. It shouts back: "No! I am Alive!"
- `Node C` hears both messages. Which one is true?
- If C trusts "Alive", it might try to connect to a dead node.
- If C trusts "Suspect", `Node A` keeps getting kicked out of the cluster even though it is healthy.

The `Incarnation Number` acts as a timestamp. The cluster follows a strict logic hierarchy when comparing two messages about the same node.

It follows the simple rule:
- Higher Incarnation wins. (e.g., Inc: 5 beats Inc: 4).
- If Incarnations are equal, specific states override others:
  - Confirm (Dead) overrides Suspect.
  - Suspect overrides Alive.


For more on SWIM, read [this paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf).




## Dynamically-Sharded Replicated State Machine(DS-RSM)
Rather than maintaining a monolithic metadata store (like a global Etcd/Zookeeper) or a single heavy Controller Quorum (like Kafka KRaft), we distribute the state management responsibility across the nodes themselves.

How it achieves that is as follows:
- Dynamic Sharding: Metadata is partitioned (sharded) based on resource keys (e.g., Topic ID). These shards are distributed among the compute nodes.

- Localized Consensus: Instead of one global Raft group reaching consensus for every operation in the system, it forms small, dynamic Raft groups (state machines) only for the nodes involved in a specific shard.
  - Example: If Node A, B, and C host "Shard 1", only they participate in the consensus for "Shard 1". Node D doesn't care.

- Elimination of External Dependencies: This removes the need for an external KV store. The application nodes are the data store for their own metadata.

- Linear Scalability: As you add nodes, you automatically add more CPU/Disk capacity for metadata processing. The "Controller" capacity scales linearly with the "Data" capacity. 



## How it interplays

### Decision
- Metadata `Shard #45` (The Raft Group) decides: "Topic-Blue is assigned to Node-Y."
- This is written to the Raft Log and replicated to the 3 nodes in `Shard #45`.
- This is the only place in the universe where this fact is guaranteed to be true.

### Propagation
The worker node (say, `Node-Y`) needs to know it has work to do. It does not "poll" the shard. 

It's the Shard Leader that dictates it.

- Action: The Leader of `Shard #45` sends a direct RPC command to `Node-Y`.
  - Command: `OpenTopic(Topic-Blue, Epoch=1)`
- Result: `Node-Y` spins up the resources for that topic and replies OK.
- State: `Node-Y` now has a local in-memory flag: "I own Topic-Blue."

### Propagation Clients (The "Pull + Cache")
This is where the scaling magic can happen. 
We do not broadcast topic assignments to all brokers. (Broadcasting 1 million topic assignments would kill the network).

Instead, it can be implemented as "need-to-know" basis(lazy loading)

- Client asks `Node-B`: "Connect me to Topic-Blue."
- `Node-B` checks its Cache: Empty.
- `Node-B` calculates hash (Hash(Topic) = 123). It consults its local SWIM-maintained Ring Map to see that hash 123 belongs to `Shard #45` (Leader IP: 10.0.0.5).
- `Shard #45` replies: "Node-Y."
- `Node-B` caches this: Topic-Blue -> Node-Y.
- `Node-B` routes the data to Node-Y.


## Handling Stale Cache 
This is the most important part. What happens if `Shard #45` changes its mind and moves the topic to `Node-Z`, 
but other nodes still think it is on `Node-Y`?

This is called a stale cache, and it is resolved via error handling:
- The Move: `Shard #45` moves the topic to Node-Z. Node-Y is told to close it (or crashes).
- The Mistake: `Node-B` (using stale cache) tries to send data to Node-Y.

- The Rejection:
  - Scenario A (Node-Y is alive): Node-Y receives the data. It checks its local list. "I don't own Topic-Blue anymore." It returns a specific error: NotLeaderForTopic.
  - Scenario B (Node-Y is dead): The connection times out.

- The Fix:
  - `Node-B` receives the error.
  - `Node-B` invalidates its cache entry for Topic-Blue.
  - `Node-B` asks `Shard #45` again: "Who owns Topic-Blue?"
  - `Shard #45` replies: "Node-Z."
  - `Node-B` updates cache and retries.