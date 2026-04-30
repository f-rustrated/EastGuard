# east-guard
EastGuard = zero-controller messaging system for flexible scalability + high operability. Inspired by LinkedIn's Northguard architecture.

# Code Quality

## Clippy
After every code change, run clippy + fix all errors before task done:
```sh
cargo clippy --all-targets --all-features -- -D warnings
```
All warnings = errors (`-D warnings`). No `#[allow(...)]` to suppress legitimate warnings — fix underlying issue. Use `#[allow(dead_code)]` only for code intentionally kept for future use or used only in test targets.

## Invariant Checking
State machines (e.g., `MetadataStateMachine`, `Raft`, `Topology`, `MetadataStorage`) have `#[cfg(test)] fn assert_invariants(&self)` methods that verify documented invariants at runtime. These are called automatically after every state-changing operation.

When adding or modifying a state-changing method on any state machine:
1. Ensure the method calls `assert_invariants()` (guarded by `#[cfg(test)]`) after mutation
2. If a new invariant is introduced, add its check to the component's `assert_invariants()`
3. If an invariant is documented in `.claude/rules/` but not checked in `assert_invariants()`, add it

This makes every existing and future test an invariant test automatically — no separate invariant tests needed.

If a code change introduces a new invariant (or modifies an existing one), **always ask the user for confirmation before proceeding** — regardless of the permission mode (auto, plan, default, etc.). Invariants are system-level contracts; adding or changing one affects every test and every future contributor.