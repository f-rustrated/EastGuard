//! End-to-end validation of D10's client-to-broker producer protocol.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use turmoil::Builder;
use uuid::Uuid;

use super::{NODES, host_cluster};
use crate::client::{Client, ClientError, PartitionStrategy, StoragePolicy};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientRequest, ClientResponse, DataPlaneResponse,
    OpenProducerSessionRequest, ProduceRequest,
};
use crate::data_plane::{ProduceError, ProducerAppendIdentity};
use crate::it::helpers::send_request;

fn seeds() -> Vec<SocketAddr> {
    NODES
        .iter()
        .map(|(host, port, _)| SocketAddr::new(turmoil::lookup(*host), *port))
        .collect()
}

fn one_record(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = vec![0]; // CompressionCodec::None
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
    payload.extend_from_slice(value);
    payload
}

#[test]
#[serial_test::serial]
fn duplicate_unknown_and_fenced_sessions_are_end_to_end_visible() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(90))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build();
    host_cluster(&mut sim, &NODES, |env| {
        env.node_id_suffix = Some("producer-dedup".to_string());
        env.vnodes_per_node = 16;
    });

    sim.client("producer-dedup-client", async {
        let client = Arc::new(Client::connect(seeds()).expect("client connects"));
        let topic = "producer-dedup";
        client
            .create_topic(
                topic,
                StoragePolicy {
                    retention_ms: Some(3_600_000),
                    replication_factor: 3,
                    partition_strategy: PartitionStrategy::AutoSplit,
                },
            )
            .await
            .expect("create topic");
        let detail = client.resolve_topic(topic).await.expect("resolve topic");
        let range_id = detail.ranges[0].range_id;
        let producer_id = Uuid::new_v4();

        let open_session_request1 = OpenProducerSessionRequest {
            topic_name: topic.to_string(),
            producer_id,
            session_nonce: Uuid::new_v4(),
        };
        let session = client
            .open_producer_session(open_session_request1)
            .await
            .expect("open producer session");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let payload = one_record(b"k", b"only-once");
        let append_identity = ProducerAppendIdentity {
            producer_id,
            incarnation: session.incarnation,
            expires_at: session.expires_at,
            sequence: 0,
            digest: crc32fast::hash(&payload),
        };

        let first = client
            .produce_to_range(
                topic,
                range_id,
                b"k",
                payload.clone(),
                1,
                Some(append_identity),
            )
            .await
            .expect("first append");
        let retry = client
            .produce_to_range(
                topic,
                range_id,
                b"k",
                payload.clone(),
                1,
                Some(append_identity),
            )
            .await
            .expect("retry resolves as duplicate");
        assert_eq!(retry, first, "retry must return the original position");

        let unknown = ProducerAppendIdentity {
            producer_id: Uuid::new_v4(),
            ..append_identity
        };
        let unknown_wire =
            ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
                topic_name: topic.to_string(),
                range_id,
                routing_key: b"k".to_vec(),
                data: payload.clone(),
                record_count: 1,
                producer_identity: Some(unknown),
            }));
        let mut rejected = false;
        for (host, port, _) in NODES {
            if matches!(
                send_request(host, port, unknown_wire.clone()).await,
                ClientResponse::DataPlane(DataPlaneResponse::ProduceRejected(
                    ProduceError::SessionExpired
                ))
            ) {
                rejected = true;
                break;
            }
        }
        assert!(
            rejected,
            "the write leader did not reject the unknown session"
        );

        let open_session_request2 = OpenProducerSessionRequest {
            topic_name: topic.to_string(),
            producer_id,
            session_nonce: Uuid::new_v4(),
        };
        let newer = client
            .open_producer_session(open_session_request2)
            .await
            .expect("recover producer session");
        assert!(newer.incarnation > session.incarnation);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let newer_payload = one_record(b"k", b"new-incarnation");
        let newer_request = ProducerAppendIdentity {
            incarnation: newer.incarnation,
            expires_at: newer.expires_at,
            digest: crc32fast::hash(&newer_payload),
            ..append_identity
        };
        client
            .produce_to_range(topic, range_id, b"k", newer_payload, 1, Some(newer_request))
            .await
            .expect("new incarnation becomes active on range leader");
        assert!(matches!(
            client
                .produce_to_range(topic, range_id, b"k", payload, 1, Some(append_identity))
                .await,
            Err(ClientError::ProduceRejected(ProduceError::ProducerFenced))
        ));
        Ok(())
    });

    sim.run()
}
