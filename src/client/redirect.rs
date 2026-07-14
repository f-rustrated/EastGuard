//! The redirect-follow loop — the SDK's one retry path, bounded by a [`RetryPolicy`]
//! deadline. A hint (redirect carrying an address) is followed at once; a transient
//! (no leader, unreachable, unconverged gossip) re-resolves to a seed with exponential
//! backoff; `TopicNotFound` re-resolves too (metadata may still be propagating) and is
//! reported only if the deadline expires still not-found. The deadline lives here, so
//! callers don't wrap calls in their own retry loops. See `c1_routing_and_connections.md`.
#![allow(unused_imports)]

use std::net::SocketAddr;
use std::time::Duration;

use tokio::time::Instant;

use crate::client::Client;
use crate::client::error::ClientError;
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientRequest, ClientResponse, ControlPlaneResponse, DataPlaneResponse,
};

/// Consecutive hint-follows before forcing a backoff — breaks a redirect cycle
/// (a real chain is 1–2 hops).
const MAX_FOLLOWS: u32 = 16;

/// Per-call retry budget: a deadline for the whole operation, plus the backoff that
/// smooths re-resolves.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Whole-operation budget, across all redirects and re-resolves.
    pub deadline: Duration,
    /// First re-resolve backoff; doubles up to `max_backoff`.
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            deadline: Duration::from_secs(30),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(1),
        }
    }
}

/// The response that served the call. `redirected` ⇒ the starting target was wrong
/// (a follow/re-resolve happened), so the caller can refresh its cache.
#[derive(Debug)]
pub(crate) struct Served {
    pub(crate) response: ClientResponse,
    pub(crate) redirected: bool,
}

/// What to do with a response: terminal, jump to a hinted node, or re-resolve.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Redirect {
    Done,
    Follow(SocketAddr),
    Reresolve,
    NotFound,
}

pub(crate) struct RetryState {
    pub(crate) addr: SocketAddr,
    pub(crate) backoff: Duration,
    pub(crate) follows: u32,
    pub(crate) redirected: bool,
    pub(crate) saw_not_found: bool,
    pub(crate) deadline: Instant,
}

impl RetryState {
    pub(crate) fn new(start: SocketAddr, policy: &RetryPolicy) -> Self {
        Self {
            addr: start,
            backoff: policy.initial_backoff,
            follows: 0,
            redirected: false,
            saw_not_found: false,
            deadline: Instant::now() + policy.deadline,
        }
    }

    pub(crate) fn budget(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Returns true if we should follow the hint, false if we hit the cycle limit
    pub(crate) fn try_follow(&mut self, next: SocketAddr) -> bool {
        self.redirected = true;
        self.follows += 1;
        if self.follows <= MAX_FOLLOWS {
            self.addr = next;
            true
        } else {
            false
        }
    }

    pub(crate) fn prepare_reresolve(
        &mut self,
        next_known: SocketAddr,
        max_backoff: Duration,
    ) -> Duration {
        self.redirected = true;
        let current_backoff = self.backoff;
        self.backoff = (self.backoff * 2).min(max_backoff);
        self.follows = 0;
        self.addr = next_known;
        current_backoff
    }

    pub(crate) fn mark_not_found(&mut self) {
        self.saw_not_found = true;
        self.redirected = true;
    }

    pub(crate) fn mark_redirected(&mut self) {
        self.redirected = true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, control_plane::NodeAddressInfo};

    use crate::control_plane::NodeAddress;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn info(port: u16) -> NodeAddressInfo {
        NodeAddressInfo {
            node_id: NodeId::new(format!("node-{port}")),
            addr: NodeAddress::test(addr(port), addr(port)),
        }
    }

    fn classify(response: &ClientResponse) -> &'static str {
        match Client::redirect_target(response) {
            Redirect::Done => "done",
            Redirect::Follow(_) => "follow",
            Redirect::Reresolve => "reresolve",
            Redirect::NotFound => "notfound",
        }
    }

    #[test]
    fn control_plane_redirects_map_to_actions() {
        assert_eq!(
            classify(&ClientResponse::ControlPlane(
                ControlPlaneResponse::TopicMetadataRedirect { owner: info(8081) }
            )),
            "follow"
        );
        assert_eq!(
            classify(&ClientResponse::ControlPlane(
                ControlPlaneResponse::NotRaftLeader {
                    leader_addr: Some(info(8082))
                }
            )),
            "follow"
        );
        assert_eq!(
            classify(&ClientResponse::ControlPlane(
                ControlPlaneResponse::NotRaftLeader { leader_addr: None }
            )),
            "reresolve"
        );
        assert_eq!(
            classify(&ClientResponse::ControlPlane(
                ControlPlaneResponse::TopicNotFound
            )),
            "notfound"
        );
        assert_eq!(
            classify(&ClientResponse::ControlPlane(
                ControlPlaneResponse::TopicCreated
            )),
            "done"
        );
    }

    #[test]
    fn data_plane_redirects_map_to_actions() {
        assert_eq!(
            classify(&ClientResponse::DataPlane(
                DataPlaneResponse::NotWriteLeader {
                    leader_addr: Some(info(8083))
                }
            )),
            "follow"
        );
        assert_eq!(
            classify(&ClientResponse::DataPlane(
                DataPlaneResponse::ShardNotLocal { hint_node: None }
            )),
            "reresolve"
        );
        assert_eq!(
            classify(&ClientResponse::DataPlane(DataPlaneResponse::TopicNotFound)),
            "notfound"
        );
        assert_eq!(
            classify(&ClientResponse::DataPlane(
                DataPlaneResponse::SegmentNotLocal
            )),
            "done"
        );
        assert_eq!(
            classify(&ClientResponse::DataPlane(DataPlaneResponse::Produced {
                entry_id: 7.into()
            })),
            "done"
        );
    }
}
