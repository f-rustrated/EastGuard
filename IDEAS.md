# Ideas

## Architecture

- **Actor model** (without global view) will be enough

## Membership

- **SWIM** protocol for membership management

## Metadata Replication

- **RAFT** without heartbeat
  - Using RAFT heartbeat to detect failed nodes can conflict with the membership management protocol
- When a member of the Raft group fails, select the next candidate (leader or follower) in a clockwise manner