# east-guard
EastGuard = zero-controller messaging system for flexible scalability + high operability. Inspired by LinkedIn's Northguard architecture.


# Rule Routing
When working on specific tasks, MUST read skill files before writing code:

- **Scheduler**: System needs trigger event later → read `rules/scheduler-skill.md`.
- **Swim**: Cluster membership logics need update → read `rules/swim-skill.md`.
- **Raft**: Consensus, leader election, log replication, or shard state machine logics need update → read `rules/raft-skill.md`.
- **Raft Actor**: Wiring Raft to network, managing shard group lifecycle, or integrating Raft with SWIM → read `rules/raft-actor-skill.md`.


# Code Quality

## Clippy
After every code change, run clippy + fix all errors before task done:
```sh
cargo clippy --all-targets --all-features -- -D warnings
```
All warnings = errors (`-D warnings`). No `#[allow(...)]` to suppress legitimate warnings — fix underlying issue. Use `#[allow(dead_code)]` only for code intentionally kept for future use or used only in test targets.