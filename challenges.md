# Ownership

## How does the system decide which server handles the connection?
If you have 1 million topics and 10 brokers, you cannot store a database row for every single topic assignment 
(e.g., Topic1 -> BrokerA). It creates too much metadata to manage and update instantly.

Instead, the system uses an intermediate abstraction layer called Bundles (Hash Ranges).

- The Scope (Namespace): The cluster is divided into logical groupings (e.g., Tenant/App1). Each Namespace acts as its own isolated container.
- The Ring: Within a Namespace, the possible key space (0 to Integer.MAX) is divided into ranges called Bundles.
- The Rule: Brokers do not own topics directly; Brokers own Bundles.


### The Scenario: Client Connects to NodeA
A client wants to Publish/Subscribe to `Topic-X`. It opens a TCP connection to `NodeA`.

- Step 1: The client does not immediately send data. It first sends a Lookup Command to `NodeA`.
- Step 2: The Hash and Mapping: `NodeA` performs a calculation:
  - Hash: Hash(Topic-X) = 12345
  - Locate range: 12345 falls into range-Z.
  - Consult Metadata: Who owns range-Z? (This data is usually cached in memory on all brokers and backed by ZooKeeper/Etcd).
- Step 3: 
  - (Hot Bundle) If it finds is owner(say either `NodeA` or `NodeB`) -> `NodeA` notifies Client of hosting node information (IP or the like).
  - (Cold Bundle) If no one owns it, the `Load Manager` selects the least-loaded node, assigns the bundle to it and then `NodeA` notifies Client of that information.
    - Every broker holds the `Load Manager`


## Data Storage
Where do the actual bytes live?

### Separation of Compute and Storage
- Broker (Compute): "Stateless". Handles TCP connections, authentication, and routing. Does not store data.
- Bookie (Storage): Stateful. Handles writing bytes to disk. Does not know about topics or clients.


### Segments (Ledgers)
A single topic is not a single file. It is a stream of data chopped into small blocks called Segments.

- Dynamic Placement:
  - Segment #1 might be stored on Bookie-1, Bookie-2, Bookie-3.
  - Segment #2 might be stored on Bookie-4, Bookie-5, Bookie-6.

The Client's View: The Client is blind to this. The Client talks only to the Broker. 
The Broker acts as the "Gateway," reading from whichever Bookie holds the current segment.



## Replication & Reliability
What happens when things break?

### What is Replicated?
- Brokers: NOT Replicated. Because they are stateless, we don't need to copy their state. If `Broker-A` dies, Broker-B just takes over ownership of the Bundle.
- Bookies: Replicated. This is where the data lives. We can consider replication based on Raft, Quorum

### Failure Scenarios
- If a Bookie crashes: No service interruption. The Broker just writes/reads from the other surviving Bookies. The system eventually auto-heals by copying data to a fresh Bookie.

- If a Broker crashes: The TCP connection drops. The Client reconnects. 
  - The Lookup Service assigns the bundle to a new Broker. The new Broker connects to the existing Bookies. Zero data is moved/copied.
  - Failure detection/propagation is baserd on the contract such as heartbeat(to determine failure) and watchers(to propagate the failures)


### What if `Broker-A` didn't crash, but just froze (GC Pause) for 10 seconds?
Imagine the situation where:  
- ZooKeeper thinks `Broker-A` is dead.
- `Broker-B` takes over and starts writing to the Bookies.
- `Broker-A` "wakes up" and thinks it is still the owner. It tries to write data. This would corrupt the file.

This is "split brain" situation. To prevent this, we need to introduce "fencing(epoch)" mechanism to bookies where:
- When `Broker-B` takes over, it tells the Bookies: "I am the owner now. My Epoch ID is 10."
- The Bookies record this: "Current Epoch for this Segment is 10."
- `Broker-A` (who thinks the Epoch is still 9) tries to write.
- The Bookies check the ID: "You are sending Epoch 9, but we are already on Epoch 10."
- Rejection: The Bookies reject the write. `Broker-A` realizes it is no longer the leader and shuts down.

Be ware the metadata store such as Zookeeper is the ultimate authority that assigns the epoch ID. 