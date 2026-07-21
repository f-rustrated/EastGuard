use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use turmoil::Builder;

use crate::control_plane::consensus::actor::MultiRaftActor;
use crate::control_plane::consensus::messages::EnsureGroup;
use crate::control_plane::consensus::transport::RaftTransportActor;
use crate::control_plane::membership::actor::SwimActor;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::control_plane::metadata::CreateTopic;
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::control_plane::{NodeId, Replicas};
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};

use super::{CLUSTER_PORT, mock_swim_handler};

const RESULT_PORT: u16 = 49000;
const GROUP_ID: ShardGroupId = ShardGroupId(180);

async fn run_node(name: &'static str, ordinal: u16) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(name);
    let all = ["node-1", "node-2", "node-3"];
    let address_map: HashMap<NodeId, SocketAddr> = all
        .iter()
        .enumerate()
        .map(|(index, node)| {
            (
                NodeId::new(*node),
                SocketAddr::new(turmoil::lookup(*node), CLUSTER_PORT + index as u16 + 1),
            )
        })
        .collect();
    let (raft_tx, raft_rx) = MultiRaftActor::channel(100);
    let (transport_tx, transport_rx) = mpsc::channel(100);
    let (swim_tx, swim_rx) = SwimActor::channel(64);
    let listener = TcpListener::bind(format!("0.0.0.0:{}", CLUSTER_PORT + ordinal)).await?;
    tokio::spawn(mock_swim_handler(swim_rx, address_map));
    let ticker = spawn_scheduling_actor(
        raft_tx.clone().into(),
        64,
        TICK_PERIOD_100_MS,
        Some(PROBE_INTERVAL_TICKS),
    );
    tokio::spawn(RaftTransportActor::run(
        node_id.clone(),
        listener,
        raft_tx.clone(),
        transport_rx,
        swim_tx.clone(),
    ));
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node_id.hash(&mut hasher);
    let (data_tx, _) = mpsc::channel(1);
    MultiRaftActor::spawn(
        ticker,
        raft_rx,
        node_id.clone(),
        hasher.finish(),
        Box::new(MetadataStorage::open(
            std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()),
        )),
        transport_tx,
        swim_tx,
        data_tx,
        super::stub_topology_reader(&all),
        2,
    );
    raft_tx
        .send(EnsureGroup {
            group: ShardGroup {
                id: GROUP_ID,
                replicas: Replicas::new(all.map(NodeId::new).into()),
            },
        })
        .await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    if raft_tx.get_leader(GROUP_ID).await.as_ref() == Some(&node_id) {
        raft_tx
            .submit_metadata_command(
                GROUP_ID,
                CreateTopic {
                    name: "snapshotted".into(),
                    storage_policy: StoragePolicy {
                        retention_ms: None,
                        replication_factor: 1,
                        partition_strategy: PartitionStrategy::Fixed,
                    },
                    replica_set: Replicas::new(vec![node_id]),
                    created_at: 1,
                }
                .into(),
            )
            .await?;
    }

    tokio::time::sleep(Duration::from_secs(30)).await;
    let topics = raft_tx.get_topics().await;
    let result = TcpListener::bind(format!("0.0.0.0:{}", RESULT_PORT + ordinal)).await?;
    let (mut stream, _) = result.accept().await?;
    let visible = topics.iter().any(|topic| topic == "snapshotted");
    stream.write_all(&[u8::from(visible)]).await?;
    Ok(())
}

#[test]
#[serial_test::serial]
fn lagging_replica_installs_compacted_snapshot() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(60))
        .build();
    for (name, ordinal) in [("node-1", 1), ("node-2", 2), ("node-3", 3)] {
        sim.host(name, move || run_node(name, ordinal));
    }
    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        turmoil::partition("node-3", "node-1");
        turmoil::partition("node-3", "node-2");
        tokio::time::sleep(Duration::from_secs(15)).await;
        turmoil::repair("node-3", "node-1");
        turmoil::repair("node-3", "node-2");
        tokio::time::sleep(Duration::from_secs(25)).await;

        let mut visible = Vec::new();
        for (name, ordinal) in [("node-1", 1), ("node-2", 2), ("node-3", 3)] {
            let mut stream = TcpStream::connect((turmoil::lookup(name), RESULT_PORT + ordinal))
                .await
                .unwrap();
            visible.push(tokio::io::AsyncReadExt::read_u8(&mut stream).await.unwrap() == 1);
        }
        assert_eq!(visible, vec![true, true, true]);
        Ok(())
    });
    sim.run()
}
