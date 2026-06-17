# east-guard
EastGuard = zero-controller messaging system for flexible scalability + high operability. Inspired by LinkedIn's Northguard architecture.

# 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

# 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

# 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

# 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

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