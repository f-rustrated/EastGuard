# EastGuard Changelog

All notable changes to this project will be documented in this file. 

## Unreleased (Main)

### SWIM & Cluster Membership
- **[PR #96]** refactor: add SwimActorCommand to decouple protocol commands from queries
- **[PR #70]** SWIM membership as authoritative source for address mapping
- **[PR #65]** feat: apply shard leader state from gossip to local topology
- **[PR #62]** feat: piggyback shard leader info onto SWIM packets (#35)
- **[PR #47]** feat: SWIM → Raft membership integration (Phase 5, #39)
- **[PR #33]** Allow dead nodes to rejoin by using fresh node id
- **[PR #21]** Add feature to join seed nodes
- **[PR #17]** feat: swim - ticker separation
- **[PR #16]** Introduce logical clock for swim actor progression
- **[PR #14]** hardening SWIM - gossip buffer impl + byte bugetting for UDP
- **[PR #9]** Implement consistent hashing cluster topology and minimal SWIM integration
- **[PR #8]** feat: swim revamp
- **[PR #6]** feat: peer discovery + swim protocol

### Control Plane & Metadata
- **[PR #198]** feat: event-driven range load classification for deterministic split/merge
- **[PR #131]** feat: peer/segment reconciliation on missing membership change
- **[PR #98]** feat: hot range detection + auto-split/merge (Phase 6)
- **[PR #94]** Fix: Size `shard_leader_buffer` based on topology configuration
- **[PR #90]** feat: metadata layer - Leader forwarding + Shard discovery
- **[PR #89]** feat: metadata management - Client -> Raft pathway
- **[PR #88]** metadata layer impl - phase3 state application
- **[PR #87]** feat: metadata phase2
- **[PR #84]** chore: post-refactoring on phase1 metadata model
- **[PR #83]** remove state_machine
- **[PR #82]** feat: metadata layer phase 1 - modelling
- **[PR #81]** Add data models for state machine

### Data Plane & Storage
- Graduate consumer-offset replicas through durable range snapshots before they can serve reads, while allowing joining replicas to receive live commits without delaying client acknowledgement.
- **[PR #136]** feat: 135
- **[PR #132]** feat: d4 - cold path
- **[PR #126]** feat: d3 - node membership reconcilation
- **[PR #125]** D3: DataPlane coordinator routing, output buffers, channel wiring (Batch 3)
- **[PR #123]** D3: Segment lifecycle integration (Batches 1-2)
- **[PR #119]** feat: entry-id offset model
- **[PR #117]** feat: D2 segment replication
- **[PR #111]** feat: deferred acknowledgement for sync (+replication placeholder)
- **[PR #109]** feat: Segment Cache Lock-Free RIngBuffer implementation
- **[PR #71]** Implement storage layer(4/6)
- **[PR #61]** Implement storage layer(3/6)
- **[PR #60]** Implement storage layer(2/6)
- **[PR #59]** Implement storage layer(1/6)
- **[PR #43]** Implement FileLogStore

### Raft & Consensus
- **[PR #120]** fix: re-randomize election jitter to prevent vote-split livelock
- **[PR #86]** Remove AddPeer, RemovePeer from RaftCommand
- **[PR #63]** fix reply, raft actor as async shell
- **[PR #58]** feat: emit LeaderChangeEvent from Raft elections + unify event buffers
- **[PR #57]** perf: aggregate raft outbound by physical node, batch TCP writes
- **[PR #46]** feat: fin phase 3, it
- **[PR #44]** ShardRaftManager + Transport (DS-RSM Phase 3)
- **[PR #41]** raft implementation
- **[PR #40]** shard group abstraction

### Client & Protocol
- **[PR #122]** opt: batch optimization in comms
- **[PR #121]** feat(client): PR 1 — wire protocol, ClientHandler dispatch, Admin & ControlPlane handlers

### Core, Infrastructure & Docs
- **[PR #134]** fix: issue 133
- **[PR #128]** docs on metadata management invars
- **[PR #127]** misc: refactor
- **[PR #118]** fix: resolve ProposeResponse::Success timing and flaky metadata_visible test
- **[PR #114]** docs(client): d1 client layer spec with protocol flows
- **[PR #113]** docs: d2 dataplane roadmap
- **[PR #112]** feat: sim test suite and transport hardening
- **[PR #110]** reformat test packages
- **[PR #106]** chore: shadowing rule
- **[PR #105]** docs storage engine phase detail
- **[PR #104]** docs: ellabs on data plane
- **[PR #103]** docs: code review skill
- **[PR #102]** code convention rule, refactoring
- **[PR #101]** docs: datanode roadmap, phases
- **[PR #99]** feat: timer as wallclock accessor
- **[PR #97]** separate commands
- **[PR #95]** Remove eastguard design
- **[PR #92]** feat: invariant testing
- **[PR #91]** fix flaky test
- **[PR #85]** Add discord server
- **[PR #80]** add debug assertions and error logs
- **[PR #73]** docs: metadata modelling - missing partition strategy
- **[PR #72]** ShardKey -> GroupKey
- **[PR #69]** chore: unnest ring
- **[PR #67]** doc: metadata management roadmap
- **[PR #66]** doc: DS-RSM high level pic
- **[PR #64]** docs: stoage plan
- **[PR #56]** add skill to use REST
- **[PR #55]** misc: 49-54
- **[PR #45]** chore: build-actor skill
- **[PR #34]** add skill routing, claude.md
- **[PR #31]** Removing randomness
- **[PR #30]** Add EastGuard-design as git submodule
- **[PR #29]** Add diagram.md
- **[PR #28]** test: enable static analysis, diable direct use of net
- **[PR #27]** integration test using turmoil
- **[PR #26]** move print to tracing
- **[PR #25]** feat: proxy ping
- **[PR #23]** Add ci script to run test
- **[PR #20]** Move dynamic dispatch to static
- **[PR #19]** feat: Scheduling actor
- **[PR #18]** chore: flush out pending events
- **[PR #12]** Introduce NodeId as the canonical node identifier
