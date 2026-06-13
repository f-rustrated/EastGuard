//! Crash recovery: rebuild local state from disk before serving.
//!
//! Design: `diagrams/data-plane/d5_crash_recovery.md`. Commit-by-commit
//! plan: `diagrams/data-plane/d5_implementation_plan.md`.
//!
//! Staged module: pieces land bottom-up (scanners → replay → orchestrator)
//! and stay unused by the running system until the orchestrator wires them
//! into node startup.

pub(crate) mod index_rebuild;
pub(crate) mod replay;
pub(crate) mod segment_scan;
pub(crate) mod wal_scan;
