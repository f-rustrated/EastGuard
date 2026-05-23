use std::collections::BTreeMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::clusters::swims::{SwimActorCommand, SwimQueryCommand};
use crate::clusters::{NodeAddress, NodeId};

pub(super) const CLUSTER_PORT: u16 = 19000;
pub(super) const QUERY_PORT: u16 = 29000;

pub(super) async fn mock_swim_handler(
    mut rx: mpsc::Receiver<SwimActorCommand>,
    address_map: BTreeMap<NodeId, SocketAddr>,
) {
    while let Some(cmd) = rx.recv().await {
        if let SwimActorCommand::Query(SwimQueryCommand::ResolveAddress { node_id, reply }) = cmd {
            let _ = reply.send(address_map.get(&node_id).map(|&addr| NodeAddress {
                cluster_addr: addr,
                client_addr: addr,
            }));
        }
    }
}

mod election;
mod leader_event;
mod membership_change;
