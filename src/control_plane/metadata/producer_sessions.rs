use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::impl_new_struct_wrapper;

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSessionMeta {
    pub(crate) incarnation: u32,
    pub(crate) expires_at: u64,
    session_nonce: uuid::Uuid,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSessions(HashMap<uuid::Uuid, ProducerSessionMeta>);
impl_new_struct_wrapper!(ProducerSessions,HashMap<uuid::Uuid, ProducerSessionMeta>);

impl ProducerSessions {
    pub(crate) fn open_producer_session(
        &mut self,
        producer_id: uuid::Uuid,
        session_nonce: uuid::Uuid,
        observed_at: u64,
        session_timeout_ms: u64,
    ) -> ProducerSessionMeta {
        self.expire_producer_sessions(observed_at);

        let incarnation = match self.get(&producer_id) {
            Some(session) if session.session_nonce == session_nonce => session.incarnation,
            Some(session) => session.incarnation.saturating_add(1),
            None => 0,
        };
        let expires_at = observed_at.saturating_add(session_timeout_ms);
        let session = ProducerSessionMeta {
            incarnation,
            expires_at,
            session_nonce,
        };
        self.insert(producer_id, session);
        session
    }

    pub(crate) fn has_expired_producer_sessions(&self, observed_at: u64) -> bool {
        self.values()
            .any(|session| session.expires_at < observed_at)
    }

    pub(crate) fn expire_producer_sessions(&mut self, observed_at: u64) {
        self.retain(|_, session| session.expires_at >= observed_at);
    }
}

#[test]
fn producer_session_recovery_bumps_incarnation_and_expiry_removes_it() {
    let mut producer_sessions = ProducerSessions::default();
    let producer_id = uuid::Uuid::new_v4();
    let first_nonce = uuid::Uuid::new_v4();

    let session = producer_sessions.open_producer_session(producer_id, first_nonce, 10, 100);
    assert_eq!((session.incarnation, session.expires_at), (0, 110));
    let session2 = producer_sessions.open_producer_session(producer_id, first_nonce, 20, 100);
    assert_eq!((session2.incarnation, session2.expires_at), (0, 120));
    let session3 =
        producer_sessions.open_producer_session(producer_id, uuid::Uuid::new_v4(), 20, 100);
    assert_eq!((session3.incarnation, session3.expires_at), (1, 120));
    assert!(producer_sessions.has_expired_producer_sessions(121));

    producer_sessions.expire_producer_sessions(121);
    assert!(!producer_sessions.contains_key(&producer_id));
}
