use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::control_plane::membership::{
    QueryCommand, SwimActorCommand, Topology, TopologyConfig, TopologyReader, topology_channel,
};
use crate::control_plane::{NodeAddress, NodeId};

pub(super) const CLUSTER_PORT: u16 = 19000;
pub(super) const QUERY_PORT: u16 = 29000;

pub(super) async fn mock_swim_handler(
    mut rx: mpsc::Receiver<SwimActorCommand>,
    address_map: HashMap<NodeId, SocketAddr>,
) {
    while let Some(cmd) = rx.recv().await {
        if let SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id, reply }) = cmd {
            let _ = reply.send(
                address_map
                    .get(&node_id)
                    .map(|&addr| NodeAddress::test(addr, addr)),
            );
        }
    }
}

/// Stub topology reader for integration tests that use `mock_swim_handler`
/// instead of a real SwimActor. Seeded with all `peer_names` so any per-group
/// queries (`live_nodes`, `ring_replacements_for`) see the test's intended
/// cluster. The publisher half is dropped on return — these tests don't
/// exercise topology mutation, and the reader's own Arc keeps the underlying
/// `ArcSwap` alive.
pub(super) fn stub_topology_reader(peer_names: &[&str]) -> TopologyReader {
    let topology = Topology::new(
        peer_names.iter().map(|n| NodeId::new(*n)),
        TopologyConfig {
            vnodes_per_pnode: 4,
            replication_factor: 3,
        },
    );
    let (_pub_handle, reader) = topology_channel(topology);
    reader
}

mod election;
mod leader_event;
mod membership_change;
