//! Consumer-side library — pure state machine for tracking a consumer's
//! per-range cursors and following lineage transitions in-band.
//!
//! D4 scope: single-consumer range tracking. No I/O; no async; no actor.
//! Caller drives by feeding fetch responses into
//! `CursorSet::apply_fetch_result`. The library hands back a `CursorAction`
//! that says what changed; the caller schedules the next fetch.
//!
//! Consumer-group concerns (durable offset commits, assignment, rebalance)
//! land in a future phase. D4 establishes the cursor-set semantics that
//! protocol will rely on.
//!
//! `#![allow(dead_code)]` — D4 lands the library; integration tests (task
//! 8) and the eventual D6 client SDK are the first non-test consumers.

#![allow(dead_code, unused_imports)]

pub(crate) mod bootstrap;
pub(crate) mod cursor;
pub(crate) mod cursor_set;
mod parked_merges;
pub use bootstrap::{KeyInterest, StartPolicy};
pub(crate) use cursor::RangeCursor;
pub(crate) use cursor_set::RangeCursorSet;
use parked_merges::*;
