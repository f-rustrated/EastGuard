use std::net::SocketAddr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use rcgen::{CertificateParams, KeyPair, SanType, string::Ia5String};
use rustls::pki_types::PrivatePkcs8KeyDer;

use crate::StartUp;
use crate::client::{
    Client, ClientSuccess, EntryId, PartitionStrategy, RangeId, RetryPolicy, StoragePolicy,
};
use crate::config::SecurityMode;
use crate::connections::protocol::{
    AdminRequest, ClientDataPlaneRequest, ClientRequest, ClientResponse, ControlPlaneRequest,
    FetchByIdRequest, NodeState, ServerError,
};
use crate::connections::{reader::ClientStreamReader, writer::ClientRawWriter};
use crate::control_plane::{
    NodeId,
    membership::{Topology, TopologyConfig},
    metadata::TopicId,
};
use crate::it::helpers::default_env;
use crate::net::TransportTcpStream;

const NODES: [&str; 3] = ["secure-1", "secure-2", "secure-3"];
const TOPIC: &str = "secure-recovery";

fn pem(label: &str, bytes: &[u8]) -> String {
    format!(
        "-----BEGIN {label}-----\n{}\n-----END {label}-----\n",
        data_encoding::BASE64.encode(bytes)
    )
}

async fn request(
    tls: Arc<rustls::ClientConfig>,
    address: SocketAddr,
    request: ClientRequest,
) -> anyhow::Result<ClientResponse> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let stream = TransportTcpStream::connect_client(address, Some(tls)).await?;
        let (read, write) = stream.into_split();
        ClientRawWriter::new(write).write(0, &request).await?;
        Ok(ClientStreamReader::new(read)
            .read_request::<ClientResponse>()
            .await?
            .1)
    })
    .await?
}

async fn assert_records(
    tls: Arc<rustls::ClientConfig>,
    address: SocketAddr,
    topic_id: TopicId,
    records: &[(EntryId, u8)],
) {
    tokio::time::timeout(Duration::from_secs(120), async {
        for &(entry_id, byte) in records {
            let fetch =
                ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
                    topic_id,
                    range_id: RangeId(0),
                    entry_id,
                    max_bytes: 4096,
                }));
            loop {
                if let Ok(ClientResponse::Ok(ClientSuccess::Fetched { entries, .. })) =
                    request(tls.clone(), address, fetch.clone()).await
                    && entries
                        .first()
                        .is_some_and(|entry| entry.data[..] == [byte; 1024])
                {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("replica {address} did not restore its acknowledged records over mTLS")
    });
}

impl Client {
    async fn assert_committed_records(
        &self,
        tls: Arc<rustls::ClientConfig>,
        records: &[(EntryId, u8)],
    ) -> anyhow::Result<()> {
        let topic = self.resolve_topic(TOPIC).await?;
        let range = topic
            .ranges
            .iter()
            .find(|range| range.range_id == RangeId(0))
            .unwrap();
        for record in records {
            let segment = range.find_segment_for_offset(record.0).unwrap();
            assert!(!segment.replica_set.is_empty());
            for replica in &segment.replica_set {
                assert_records(
                    tls.clone(),
                    replica.addr.client_addr,
                    topic.topic_id,
                    std::slice::from_ref(record),
                )
                .await;
            }
        }
        Ok(())
    }

    async fn secure_test_produce(&self, byte: u8) -> EntryId {
        for _ in 0..20 {
            if let Ok(id) = self
                .produce_to_range(TOPIC, RangeId(0), b"k", vec![byte; 1024], 1, None)
                .await
            {
                return id;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("mTLS produce did not recover");
    }
}

#[test]
#[serial_test::serial]
fn mtls_formation_partition_and_persistent_restart() -> turmoil::Result {
    let files = tempfile::tempdir()?;
    let mut sim = turmoil::Builder::new()
        .rng_seed(21)
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(500))
        .tcp_capacity(64 * 1024)
        .build();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let topology = Topology::new(
        NODES.map(|node| NodeId::new(format!("{node}::sim"))),
        TopologyConfig {
            vnodes_per_pnode: 8,
            replication_factor: 3,
        },
    );
    let topic_id = TopicId(topology.shard_group_for(TOPIC.as_bytes()).unwrap().id.0 << 32);
    let policy_path = files.path().join("permissions.json");
    std::fs::write(
        &policy_path,
        serde_json::to_vec(&serde_json::json!({
            "topics": {TOPIC: topic_id.0},
            "grants": {"cluster": ["operator"], format!("topic-data/{}", topic_id.0): ["operator"]}
        }))?,
    )?;

    let mut roots = rustls::RootCertStore::empty();
    let mut trust_pem = String::new();
    let addresses: Vec<SocketAddr> = NODES
        .iter()
        .map(|node| SocketAddr::new(sim.lookup(*node), 8081))
        .collect();
    let mut identities = Vec::new();
    for (i, principal) in NODES.into_iter().chain(["operator"]).enumerate() {
        let key = KeyPair::generate()?;
        let mut params = CertificateParams::default();
        let role = if i == NODES.len() { "client" } else { "node" };
        params
            .subject_alt_names
            .push(SanType::URI(Ia5String::try_from(format!(
                "urn:eastguard:{role}:{principal}"
            ))?));
        if let Some(addr) = addresses.get(i) {
            params.subject_alt_names.push(SanType::IpAddress(addr.ip()));
        }
        let certificate = params.self_signed(&key)?;
        roots.add(certificate.der().clone())?;
        trust_pem.push_str(&pem("CERTIFICATE", certificate.der()));
        identities.push((certificate, key));
    }
    let trust_path = files.path().join("roots.pem");
    std::fs::write(&trust_path, trust_pem)?;
    let (client_certificate, client_key) = identities.pop().unwrap();
    let tls = Arc::new(
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_client_auth_cert(
                vec![client_certificate.der().clone()],
                PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
            )?,
    );

    for (i, (certificate, key)) in identities.into_iter().enumerate() {
        let name = NODES[i];
        let directory = files.path().join(name);
        std::fs::create_dir_all(&directory)?;
        let certificate_path = directory.join("certificate.pem");
        let key_path = directory.join("key.pem");
        std::fs::write(&certificate_path, pem("CERTIFICATE", certificate.der()))?;
        std::fs::write(&key_path, pem("PRIVATE KEY", key.serialized_der()))?;
        let mut env = default_env(i as u32, name.into(), 8081, 18001);
        env.security_mode = SecurityMode::Secure;
        env.certificate_chain_path = Some(certificate_path);
        env.private_key_path = Some(key_path);
        env.trust_root_path = Some(trust_path.clone());
        env.permissions_path = Some(policy_path.clone());
        env.node_id_suffix = Some("sim".into());
        env.vnodes_per_node = 8;
        env.segment_size_limit_bytes = 2048;
        env.raft_snapshot_entry_threshold = 4;
        env.config_dir = directory.join("config").to_string_lossy().into_owned();
        env.meta_dir = directory.join("metadata").to_string_lossy().into_owned();
        env.data_dir = directory.join("data").to_string_lossy().into_owned();
        env.advertise_host = Some(addresses[i].ip().to_string());
        env.join_seed_nodes = addresses
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, address)| format!("{}:18001", address.ip()))
            .collect();
        sim.host(name, move || {
            let env = env.clone();
            async move {
                StartUp::with_env(env, i as u64).run().await?;
                Ok(())
            }
        });
    }

    let stage = Arc::new(AtomicUsize::new(0));
    let driver = stage.clone();
    sim.client("secure-client", async move {
        // All client traffic uses the SDK's native mTLS path, including redirects.
        tokio::time::sleep(Duration::from_secs(15)).await;
        for address in &addresses {
            let response = request(
                tls.clone(),
                *address,
                ClientRequest::Admin(AdminRequest::DescribeCluster),
            )
            .await?;
            let ClientResponse::Ok(ClientSuccess::ClusterInfo { nodes }) = response else {
                panic!("cluster query failed: {response:?}")
            };
            assert_eq!(
                nodes
                    .iter()
                    .filter(|node| node.state == NodeState::Alive)
                    .count(),
                3
            );
        }
        let client =
            Client::connect_secure(addresses.clone(), RetryPolicy::default(), tls.clone())?;
        client
            .create_topic(
                TOPIC,
                StoragePolicy {
                    replication_factor: 3,
                    retention_ms: None,
                    partition_strategy: PartitionStrategy::Fixed,
                },
            )
            .await?;
        assert_eq!(client.resolve_topic(TOPIC).await?.topic_id, topic_id);
        let mut records = Vec::new();
        for byte in b"abc" {
            records.push((client.secure_test_produce(*byte).await, *byte));
        }
        assert_eq!(records[0].0, EntryId(0));

        let denied = request(
            tls.clone(),
            addresses[0],
            ClientRequest::ControlPlane(ControlPlaneRequest::DeleteTopic { name: TOPIC.into() }),
        )
        .await?;
        assert!(matches!(
            denied,
            ClientResponse::Err(ServerError::Unauthorized)
        ));

        for peer in &NODES[..2] {
            turmoil::partition(NODES[2], *peer);
        }
        tokio::time::sleep(Duration::from_secs(35)).await;
        let survivors =
            Client::connect_secure(addresses[..2].to_vec(), RetryPolicy::default(), tls.clone())?;
        let id = survivors.secure_test_produce(b'd').await;
        assert!(id >= EntryId(3));
        records.push((id, b'd'));
        for peer in &NODES[..2] {
            turmoil::repair(NODES[2], *peer);
        }
        // The live tail can remain assigned to the two survivors; only sealed
        // segments are refilled when capacity returns. Verify every assigned
        // replica, and require the healed broker to regain the sealed prefix.
        survivors
            .assert_committed_records(tls.clone(), &records)
            .await?;
        assert_records(tls.clone(), addresses[2], topic_id, &records[..3]).await;

        stage.store(1, Ordering::SeqCst);
        while stage.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert_records(tls.clone(), addresses[2], topic_id, &records[..3]).await;
        let reconnected =
            Client::connect_secure(vec![addresses[2]], RetryPolicy::default(), tls.clone())?;
        assert_eq!(reconnected.resolve_topic(TOPIC).await?.topic_id, topic_id);
        records.push((reconnected.secure_test_produce(b'e').await, b'e'));
        reconnected.assert_committed_records(tls, &records).await?;
        Ok(())
    });
    while driver.load(Ordering::SeqCst) != 1 {
        assert!(
            sim.elapsed() < Duration::from_secs(280),
            "cluster never reached the restart step"
        );
        sim.step()?;
    }
    sim.crash(NODES[2]);
    let restart_at = sim.elapsed() + Duration::from_secs(35);
    while sim.elapsed() < restart_at {
        sim.step()?;
    }
    sim.bounce(NODES[2]);
    driver.store(2, Ordering::SeqCst);
    sim.run()
}
