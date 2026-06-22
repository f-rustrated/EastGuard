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

use crate::{
    control_plane::metadata::{RangeId, RangeMeta, RangeState, TopicMeta},
    data_plane::messages::query::{FetchResult, ListOffsetsResult},
    impl_from_variant,
};

/// Client → broker data-plane request. Every variant carries a `Client*Request`
/// struct so the carried fields are named in one place (consistent with the
/// project's tuple-variant + named-struct enum pattern) and the `Client`
/// prefix makes the audience explicit at the use site.
#[derive(Clone, Encode, Decode)]
pub enum ClientDataPlaneRequest {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    FetchById(FetchByIdRequest),
    ListOffsets(ListOffsetsRequest),
}

impl_from_variant!(
    ClientDataPlaneRequest,
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    FetchById(FetchByIdRequest),
    ListOffsets(ListOffsetsRequest),
);

#[derive(Clone, Encode, Decode)]
pub struct ProduceRequest {
    pub topic_name: String,
    /// Used by the server to locate the target range; never stored.
    pub routing_key: Vec<u8>,
    /// Pre-serialized blob produced by the client: a leading 1-byte cleartext
    /// codec tag (none/lz4/zstd) followed by the optionally-compressed records.
    /// The broker stamps an entry_id and stores/replicates this opaque payload as-is.
    pub data: Vec<u8>,
    pub record_count: u32,
}

#[derive(Clone, Encode, Decode)]
pub struct FetchRequest {
    pub topic_name: String,
    pub range_id: u64,
    pub entry_id: u64,
    /// Position within the entry to start from; 0 means the first record.
    pub record_index: u32,
    pub max_bytes: u32,
    /// Optional sub-range of the target range's keyspace to filter records by.
    /// A future consumer-group layer will enable server-side narrowing without a
    /// wire-format break. See d4_consumer_range_tracking.md.
    pub keyspace_bound: Option<KeyspaceBound>,
}

/// Consumer fetch addressed by resolved `topic_id` (from a prior `DescribeTopic`)
/// rather than by name. Lets any replica holding the segment serve it without
/// resolving the topic name — i.e. without being a metadata peer. The client owns
/// resolution; the server never proxies.
#[derive(Clone, Encode, Decode)]
pub struct FetchByIdRequest {
    pub topic_id: u64,
    pub range_id: u64,
    pub entry_id: u64,
    pub max_bytes: u32,
}

#[derive(Clone, Encode, Decode)]
pub struct ListOffsetsRequest {
    pub topic_name: String,
    pub range_id: u64,
}

#[derive(Debug, Encode, Decode)]
pub enum DataPlaneResponse {
    // Produce
    Produced {
        entry_id: u64,
    },
    // Fetch
    Fetched {
        entries: Box<[Entry]>,
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
    // `NotWriteLeader` is the segment's data-replica write leader (`replica_set[0]`),
    // distinct from the metadata Raft leader (`ControlPlaneResponse::NotRaftLeader`).
    NotWriteLeader {
        leader_addr: Option<SocketAddr>,
    },
    ShardNotLocal {
        hint_node: Option<SocketAddr>,
    },
    TopicNotFound,
    InternalError(String),
}

impl DataPlaneResponse {
    pub(crate) fn from_list_offset_result(value: ListOffsetsResult) -> Self {
        match value {
            ListOffsetsResult::Offsets {
                start_entry_id,
                committed_entry_id,
            } => DataPlaneResponse::Offsets {
                start_entry_id,
                committed_entry_id,
            },
            ListOffsetsResult::SegmentNotLocal => {
                DataPlaneResponse::InternalError("segment not hosted on this node".into())
            }
            ListOffsetsResult::InternalError(s) => DataPlaneResponse::InternalError(s),
        }
    }

    pub(crate) fn from_fetch_result(result: FetchResult) -> Self {
        match result {
            FetchResult::Records {
                entries,
                next_entry_id,
                progress_signal,
            } => {
                // Single Bytes → Vec<u8> copy per entry at the wire boundary
                // — bincode's owned-byte encoding requires Vec<u8>. The
                // intermediate `FetchedEntry` step that used to live in the
                // data plane is gone; the Arc rides straight through the
                // channel from the cache.
                let wire_entries = entries
                    .into_iter()
                    .map(|cached| Entry {
                        entry_id: cached.entry_id,
                        // Auto-deref EntryPayload → Bytes → [u8], then to_vec.
                        data: cached.data.to_vec(),
                        record_count: cached.record_count,
                    })
                    .collect();
                DataPlaneResponse::Fetched {
                    entries: wire_entries,
                    next_entry_id,
                    progress_signal,
                }
            }
            FetchResult::EntryIdOutOfRange => DataPlaneResponse::EntryIdOutOfRange,
            FetchResult::SegmentNotLocal => {
                // The consumer treats this as "stale targeting" — re-resolve via
                // DescribeTopic and retry against a node that hosts the segment.
                DataPlaneResponse::InternalError("segment not hosted on this node".into())
            }
            FetchResult::InternalError(s) => DataPlaneResponse::InternalError(s),
        }
    }
}

#[derive(Clone, Debug, Encode, Decode)]
pub struct KeyspaceBound {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

/// A single entry as served to the consumer.
/// The broker never parses `data` — it is stored and replicated opaque.
/// Consumers read the leading 1-byte codec tag, decompress the remainder, and
/// parse records from it using `record_count`.
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
        left_range_id: RangeId,
        right_range_id: RangeId,
        split_point: Vec<u8>,
    },
    Merged {
        merged_range_id: RangeId,
        merged_from: [RangeId; 2],
    },
}

impl RangeProgressSignal {
    /// Derive the `RangeProgressSignal` the wire ships to the consumer from the
    /// range's metadata snapshot. Active → `Active`. Sealed → `Sealed{end_offset,
    /// transition}` where `end_offset` comes from `next_offset - 1` and the
    /// transition is whichever lineage pointer is set (split vs merge).
    pub(crate) fn compute_progress_signal(range: &RangeMeta, topic: &TopicMeta) -> Self {
        if range.state == RangeState::Active {
            return RangeProgressSignal::Active;
        }
        // Sealed (or Deleting — both surface as Sealed to the consumer here; the
        // consumer's lineage walk handles them the same way).
        let end_offset = range.next_offset.saturating_sub(1);
        if let Some([left, right]) = range.split_into {
            // Split point is the right child's keyspace_start. The right child's
            // keyspace_start isn't on `range` directly, so the consumer derives
            // it from the child's `RangeDetail.keyspace_start`. Until the child
            // is fetched via DescribeTopic, we don't have it here — use the
            // range's own end as a stand-in placeholder. The consumer's lineage
            // walker re-resolves the precise split point when it discovers the
            // children.
            return RangeProgressSignal::Sealed {
                end_offset,
                transition: RangeTransition::Split {
                    left_range_id: left,
                    right_range_id: right,
                    split_point: range.keyspace_end.clone(),
                },
            };
        }
        if let Some(merged) = range.merged_into {
            // Look up M to get its merged_from.
            // ! SAFETY: failre of resolving merged_from shouldn't happen
            let merged_from = topic
                .merged_from(merged)
                .expect("Merged from not found when range.merge_into is invoked");
            return RangeProgressSignal::Sealed {
                end_offset,
                transition: RangeTransition::Merged {
                    merged_range_id: merged,
                    merged_from,
                },
            };
        }
        // Sealed with no lineage pointer — shouldn't happen given metadata
        // invariants (a sealed range is sealed BY split or merge). Fall back to
        // a "self-merged" sentinel so the consumer at least sees a Sealed
        // signal and can stop fetching past `end_offset`.
        RangeProgressSignal::Sealed {
            end_offset,
            transition: RangeTransition::Merged {
                merged_range_id: range.range_id,
                merged_from: [range.range_id, range.range_id],
            },
        }
    }
}
