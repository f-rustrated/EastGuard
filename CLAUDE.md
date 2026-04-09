# east-guard
EastGuard is a zero-controller messaging system designed for flexible scalability and high operability. This project is significantly inspired by the architecture of LinkedIn's Northguard.


# Skill Routing
When working on specific tasks, you MUST read the following skill files before writing code:

- **Scheduler**: If system needs to trigger certain event later, read `.agent-skills/scheduler-skill.md`.
- **Swim**: If cluster membership logics need to be updated, read `.agent-skills/swim-skill.md`.

# Code Quality

## Clippy
After every code change, run clippy and fix all errors before considering the task done:
```sh
cargo clippy --all-targets --all-features -- -D warnings
```
All warnings are treated as errors (`-D warnings`). Do not use `#[allow(...)]` to suppress legitimate warnings — fix the underlying issue instead. Use `#[allow(dead_code)]` only for code that is intentionally kept for future use or used only in test targets.
