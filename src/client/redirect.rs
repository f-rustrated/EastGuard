//! The redirect-follow loop — the SDK's one retry path, bounded by a [`RetryPolicy`]
//! deadline. A hint (redirect carrying an address) is followed at once; a transient
//! (no leader, unreachable, unconverged gossip) re-resolves to a seed with exponential
//! backoff; `TopicNotFound` re-resolves too (metadata may still be propagating) and is
//! reported only if the deadline expires still not-found. The deadline lives here, so
//! callers don't wrap calls in their own retry loops. See `c1_routing_and_connections.md`.

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
    pub response: ClientResponse,
    pub redirected: bool,
}

/// What to do with a response: terminal, jump to a hinted node, or re-resolve.
enum Redirect {
    Done,
    Follow(SocketAddr),
    Reresolve,
    NotFound,
}

struct RetryState {
    addr: SocketAddr,
    backoff: Duration,
    follows: u32,
    redirected: bool,
    saw_not_found: bool,
    deadline: Instant,
}

impl RetryState {
    fn new(start: SocketAddr, policy: &RetryPolicy) -> Self {
        Self {
            addr: start,
            backoff: policy.initial_backoff,
            follows: 0,
            redirected: false,
            saw_not_found: false,
            deadline: Instant::now() + policy.deadline,
        }
    }

    fn budget(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Returns true if we should follow the hint, false if we hit the cycle limit
    fn try_follow(&mut self, next: SocketAddr) -> bool {
        self.redirected = true;
        self.follows += 1;
        if self.follows <= MAX_FOLLOWS {
            self.addr = next;
            true
        } else {
            false
        }
    }

    fn prepare_reresolve(&mut self, next_known: SocketAddr, max_backoff: Duration) -> Duration {
        self.redirected = true;
        let current_backoff = self.backoff;
        self.backoff = (self.backoff * 2).min(max_backoff);
        self.follows = 0;
        self.addr = next_known;
        current_backoff
    }

    fn mark_not_found(&mut self) {
        self.saw_not_found = true;
        self.redirected = true;
    }

    fn mark_redirected(&mut self) {
        self.redirected = true
    }
}

impl Client {
    /// Drive `request` to completion from `start`, following redirects until the
    /// deadline. Returns the serving response or a terminal error.
    pub(crate) async fn call(
        &self,
        start: SocketAddr,
        request: impl Into<ClientRequest>,
    ) -> Result<Served, ClientError> {
        let mut state = RetryState::new(start, &self.retry);
        let mut last_error: Option<String> = None;

        let request = request.into();
        loop {
            // Budget spent: not-found if that's all we saw, else timeout.
            if Instant::now() >= state.deadline {
                if state.saw_not_found {
                    return Err(ClientError::TopicNotFound);
                }
                return Err(ClientError::Timeout {
                    waited: self.retry.deadline,
                    last_error,
                });
            }

            // Cap the attempt at the remaining budget: a request on an established
            // connection awaits a reply with no inner deadline, so a node that never
            // answers (parked reply, half-open socket) can't outlast the call.
            let attempt =
                tokio::time::timeout(state.budget(), self.pool.send(state.addr, request.clone()));

            let attempt_res = attempt.await;

            if attempt_res.is_err() {
                last_error = Some("request attempt timed out".to_string());
                continue;
            }

            let res = attempt_res.unwrap();

            match res {
                Ok(response) => {
                    let is_direct_to_node = matches!(
                        request,
                        ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(_))
                            | ClientRequest::DataPlane(ClientDataPlaneRequest::ListOffsets(_))
                    );

                    let redirect = if is_direct_to_node {
                        match &response {
                            ClientResponse::DataPlane(DataPlaneResponse::SegmentNotLocal) => {
                                Redirect::Done
                            }
                            ClientResponse::DataPlane(DataPlaneResponse::ShardNotLocal {
                                hint_node: None,
                            }) => Redirect::Done,
                            _ => Self::redirect_target(&response),
                        }
                    } else {
                        Self::redirect_target(&response)
                    };

                    match redirect {
                        Redirect::Done => {
                            return Ok(Served {
                                response,
                                redirected: state.redirected,
                            });
                        }
                        Redirect::Follow(next) => {
                            // The hint is a node the server just pointed us at — remember it
                            // so future re-resolves can reach it, not just the bootstrap set.
                            self.known_nodes.remember(next);
                            if state.try_follow(next) {
                                continue;
                            }
                        }
                        Redirect::NotFound => state.mark_not_found(),
                        Redirect::Reresolve => state.mark_redirected(),
                    }
                }
                // Pool dropped the dead connection; re-resolve like any transient.
                Err(ClientError::Connection { addr, reason }) => {
                    last_error = Some(format!("Connection to {} failed: {}", addr, reason));
                    state.mark_redirected()
                }
                Err(e) => return Err(e),
            }

            // Transient: back off (never past the deadline), then try a fresh seed.
            let remaining = state.budget();
            let sleep_for = state.prepare_reresolve(self.next_known_node(), self.retry.max_backoff);
            tokio::time::sleep(sleep_for.min(remaining)).await;
        }
    }

    /// Map a response to its action. All variants listed, so a new wire variant
    /// won't compile until handled.
    fn redirect_target(response: &ClientResponse) -> Redirect {
        match response {
            ClientResponse::ControlPlane(cp) => match cp {
                ControlPlaneResponse::TopicMetadataRedirect { owner } => {
                    Redirect::Follow(owner.client_addr)
                }
                ControlPlaneResponse::NotRaftLeader { leader_addr } => match leader_addr {
                    Some(info) => Redirect::Follow(info.client_addr),
                    None => Redirect::Reresolve,
                },
                ControlPlaneResponse::TopicNotFound => Redirect::NotFound,
                ControlPlaneResponse::InternalError(_) => Redirect::Reresolve,
                ControlPlaneResponse::TopicCreated
                | ControlPlaneResponse::AlreadyExists
                | ControlPlaneResponse::TopicDeleted
                | ControlPlaneResponse::TopicList { .. }
                | ControlPlaneResponse::TopicDetail(_) => Redirect::Done,
            },
            ClientResponse::DataPlane(dp) => match dp {
                DataPlaneResponse::NotWriteLeader { leader_addr } => match leader_addr {
                    Some(addr) => Redirect::Follow(*addr),
                    None => Redirect::Reresolve,
                },
                DataPlaneResponse::ShardNotLocal { hint_node } => match hint_node {
                    Some(addr) => Redirect::Follow(*addr),
                    None => Redirect::Reresolve,
                },
                DataPlaneResponse::TopicNotFound => Redirect::NotFound,
                DataPlaneResponse::InternalError(_) => Redirect::Reresolve,
                DataPlaneResponse::SegmentNotLocal
                | DataPlaneResponse::Produced { .. }
                | DataPlaneResponse::Fetched { .. }
                | DataPlaneResponse::EntryIdOutOfRange
                | DataPlaneResponse::KeyspaceBoundNarrowed
                | DataPlaneResponse::RangeOffset { .. } => Redirect::Done,
            },
            ClientResponse::Admin(_) => Redirect::Done,
            // The server's writer-loop sentinel; a client never legitimately reads it.
            ClientResponse::Stop => Redirect::Reresolve,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::protocol::NodeAddressInfo;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn info(port: u16) -> NodeAddressInfo {
        NodeAddressInfo {
            node_id: format!("node-{port}"),
            client_addr: addr(port),
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
                    leader_addr: Some(addr(8083))
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
