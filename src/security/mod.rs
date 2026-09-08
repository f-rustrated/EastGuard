mod actor;
mod certificates;
mod message;
mod record_reader;
mod state;
mod transport;

pub(crate) const MAX_SECURITY_ID_BYTES: usize = 4 * 1024;

pub(crate) use actor::{SecurityActor, SecurityHandle};
pub(crate) use certificates::{client_certificate_principal, node_certificate_principal};
pub(crate) use transport::{CertificatePrincipal, NodeTransportSecurity};
