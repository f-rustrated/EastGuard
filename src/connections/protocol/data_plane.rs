//! Data-plane wire types — produce, fetch, list-offsets, and the in-band
//! signals that drive consumer range tracking (see
//! d4_consumer_range_tracking.md).
//!
//! The client is responsible for routing to the correct node using its local
//! routing cache. When this node is not the right destination, a redirect
//! error is returned so the client can reconnect and retry. Stale targeting
//! costs a retry, never correctness.

use std::net::SocketAddr;

use bincode::{Decode, Encode};

#[derive(Clone, Encode, Decode)]
pub enum DataPlaneRequest {
    Produce {
        topic_name: String,
        // Used by the server to locate the target range; never stored.
        routing_key: Vec<u8>,
        // Pre-serialized and optionally compressed blob produced by the client.
        // The broker stamps an entry_id and stores/replicates this opaque payload as-is.
        data: Vec<u8>,
        record_count: u32,
    },
    Fetch {
        topic_name: String,
        range_id: u64,
        entry_id: u64,
        // Position within the entry to start from; 0 means the first record.
        record_index: u32,
        max_bytes: u32,
        // Optional sub-range of the target range's keyspace to filter records by.
        // A future consumer-group layer will enable server-side narrowing without a
        // wire-format break. See d4_consumer_range_tracking.md.
        keyspace_bound: Option<KeyspaceBound>,
    },
    ListOffsets {
        topic_name: String,
        range_id: u64,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum DataPlaneResponse {
    // Produce
    Produced {
        entry_id: u64,
    },
    RangeSplitting {
        left_range_id: u64,
        right_range_id: u64,
        split_point: Vec<u8>,
    },
    // Fetch
    Fetched {
        entries: Vec<Entry>,
        next_entry_id: u64,
        progress_signal: RangeProgressSignal,
    },
    EntryIdOutOfRange,
    // keyspace_bound was set but narrower than the target range's keyspace.
    KeyspaceBoundNarrowed,
    // ListOffsets
    Offsets {
        start_entry_id: u64,
        committed_entry_id: u64,
    },
    // Redirect errors — client reconnects and retries on the indicated node
    NotLeader {
        leader_addr: Option<SocketAddr>,
    },
    ShardNotLocal {
        hint_node: SocketAddr,
    },
    TopicNotFound,
    InternalError(String),
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct KeyspaceBound {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// A single entry as served to the consumer.
/// The broker never parses `data` — it is stored and replicated opaque.
/// Consumers decompress and parse records from `data` using `record_count`.
#[derive(Debug, Encode, Decode)]
pub struct Entry {
    pub entry_id: u64,
    pub data: Vec<u8>,
    pub record_count: u32,
}

/// Forward-pointing in-band signal carried on every fetch response. Denormalized
/// projection of `RangeMeta` (state + last committed offset + split/merge
/// payload) — tells the consumer whether to keep fetching this range or drain
/// to `end_offset` and follow the lineage transition to its successor(s).
/// See (d4_consumer_range_tracking.md, "Range Transitions".)
#[derive(Debug, Encode, Decode)]
pub enum RangeProgressSignal {
    Active,
    Sealed {
        end_offset: u64,
        transition: RangeTransition,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum RangeTransition {
    Split {
        left_range_id: u64,
        right_range_id: u64,
        split_point: Vec<u8>,
    },
    Merged {
        merged_range_id: u64,
    },
}
