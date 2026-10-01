//! Authentication answers who is connected; authorization answers what they may do.
//!
//! - `certificates`: parses principals and checks their node-ID namespace.
//! - `transport`: loads TLS credentials and authenticates peer connections.
//! - `acl_actor`: queues ACL requests, schedules reads, and delivers replies.
//! - `acl_reader`: routes quorum-backed reads and rejects changed ownership.
//! - `acl_state`: makes cached access decisions and coalesces cache misses, without I/O.
//!
//! Transports take [`NodeTransportSecurity`]; request handlers take [`AclHandle`].
//! Durable ACL records remain in the metadata state machine, not this cache.

mod acl_actor;
mod acl_messages;
mod acl_reader;
mod acl_state;
mod certificates;
mod transport;

pub(crate) const MAX_SECURITY_ID_BYTES: usize = 4 * 1024;

pub(crate) use acl_actor::{AclActor, AclHandle};
pub(crate) use certificates::{
    CertificatePrincipal, client_certificate_principal, node_certificate_principal,
};
pub(crate) use transport::NodeTransportSecurity;
