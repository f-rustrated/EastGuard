use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::metadata::ProducerSessionOwner;
use crate::control_plane::metadata::command::OpenProducerSession;
use crate::control_plane::metadata::error::MetadataError;
use crate::impl_new_struct_wrapper;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSessionMeta {
    pub(crate) incarnation: u32,
    pub(crate) expires_at: u64,
    pub(crate) owner: ProducerSessionOwner,
    session_nonce: uuid::Uuid,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSessions(HashMap<uuid::Uuid, ProducerSessionMeta>);
impl_new_struct_wrapper!(ProducerSessions,HashMap<uuid::Uuid, ProducerSessionMeta>);

impl ProducerSessions {
    pub(crate) fn get_for_owner(
        &self,
        producer_id: &uuid::Uuid,
        owner: &ProducerSessionOwner,
    ) -> Result<Option<&ProducerSessionMeta>, MetadataError> {
        match self.get(producer_id) {
            Some(session) if &session.owner != owner => {
                Err(MetadataError::ProducerSessionOwnerMismatch)
            }
            session => Ok(session),
        }
    }

    pub(crate) fn open_producer_session(
        &mut self,
        command: OpenProducerSession,
    ) -> Result<(), MetadataError> {
        self.expire_producer_sessions(command.observed_at);

        let incarnation = match self.get_for_owner(&command.producer_id, &command.owner)? {
            Some(session) if session.session_nonce == command.session_nonce => session.incarnation,
            Some(session) => session.incarnation.saturating_add(1),
            None => 0,
        };
        let expires_at = command
            .observed_at
            .saturating_add(command.session_timeout_ms);
        let session = ProducerSessionMeta {
            incarnation,
            expires_at,
            owner: command.owner,
            session_nonce: command.session_nonce,
        };
        self.insert(command.producer_id, session);
        Ok(())
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
    let owner = ProducerSessionOwner::CertificatePrincipal("orders-service".into());

    let command = |session_nonce, session_owner, observed_at| OpenProducerSession {
        topic_name: "orders".into(),
        producer_id,
        session_nonce,
        owner: session_owner,
        observed_at,
        session_timeout_ms: 100,
    };

    producer_sessions
        .open_producer_session(command(first_nonce, owner.clone(), 10))
        .unwrap();
    let session = &producer_sessions[&producer_id];
    assert_eq!((session.incarnation, session.expires_at), (0, 110));

    producer_sessions
        .open_producer_session(command(first_nonce, owner.clone(), 20))
        .unwrap();
    let session2 = &producer_sessions[&producer_id];
    assert_eq!((session2.incarnation, session2.expires_at), (0, 120));

    producer_sessions
        .open_producer_session(command(uuid::Uuid::new_v4(), owner.clone(), 20))
        .unwrap();
    let session3 = &producer_sessions[&producer_id];
    assert_eq!((session3.incarnation, session3.expires_at), (1, 120));
    assert_eq!(
        producer_sessions.open_producer_session(command(
            uuid::Uuid::new_v4(),
            ProducerSessionOwner::CertificatePrincipal("other-service".into()),
            20,
        )),
        Err(MetadataError::ProducerSessionOwnerMismatch)
    );
    assert_eq!(producer_sessions[&producer_id].owner, owner);
    assert!(producer_sessions.has_expired_producer_sessions(121));

    producer_sessions.expire_producer_sessions(121);
    assert!(!producer_sessions.contains_key(&producer_id));
}
