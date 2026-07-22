use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

/// Stable identity of one producer append. The range is supplied by the
/// enclosing segment key, so it is deliberately not duplicated here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct ProducerAppendIdentity {
    pub producer_id: Uuid,
    pub incarnation: u32,
    pub expires_at: u64,
    pub sequence: u64,
    pub digest: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, thiserror::Error)]
pub enum ProduceError {
    #[error("not the write leader")]
    NotLeader,
    #[error("segment not found")]
    SegmentNotFound,
    #[error("producer incarnation was fenced")]
    ProducerFenced,
    #[error("producer session expired or is unknown")]
    SessionExpired,
    #[error("sequence was reused with a different payload")]
    RequestIdentityConflict,
    #[error("duplicate position is outside the retained result window")]
    DuplicatePositionUnavailable,
    #[error("sequence gap; expected {0}")]
    SequenceGap(u64),
    #[error("the same producer request is already in flight")]
    RequestInFlight,
    #[error("internal produce failure: {0}")]
    Internal(String),
}
