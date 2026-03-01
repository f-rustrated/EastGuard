use crate::clusters::swims::actor::SwimActor;
use crate::clusters::swims::{SwimCommand, SwimTestCommand, SwimTimer};
use crate::clusters::transport::{SwimTransportActor, UdpTransport};
use crate::clusters::{JoinConfig, NodeId};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker_message::TickerCommand;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use turmoil::net::UdpSocket;

impl UdpTransport for UdpSocket {
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.send_to(buf, target).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }
}

pub struct NodeSpec {
    pub node_id: NodeId,
    pub host: &'static str,
    pub port: u16,
    pub cluster_port: u16,
}

#[allow(dead_code)]
pub struct DstNode {
    pub node_id: NodeId,
    pub cluster_addr: SocketAddr,
    pub port: u16,
    tx_in: mpsc::Sender<SwimCommand>,
    ticker_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
}

#[allow(dead_code)]
impl DstNode {
    pub async fn query_topology_count(&self) -> usize {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx_in
            .send(SwimCommand::Test(
                SwimTestCommand::TopologyValidationCount { reply: tx },
            ))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    #[allow(dead_code)]
    pub async fn query_topology_includes(&self, node_id: NodeId) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx_in
            .send(SwimCommand::Test(SwimTestCommand::TopologyIncludesNode {
                node_id,
                reply: tx,
            }))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    pub async fn force_tick(&self) {
        self.ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
    }
}

pub fn set_up_cluster(sim: &mut turmoil::Sim, specs: &[NodeSpec]) -> Vec<DstNode> {
    let n = specs.len();
    let cluster_ports: Vec<u16> = specs.iter().map(|s| s.cluster_port).collect();

    let mut dst_nodes = Vec::new();

    for (i, spec) in specs.iter().enumerate() {
        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, rx_out) = mpsc::channel(100);
        let (ticker_tx, ticker_rx) = mpsc::channel(100);

        // Wrap receivers in Arc<Mutex<Option>> because sim.host() requires Fn (not FnOnce).
        // Each receiver is taken on the first (and only) call.
        let rx_in = Arc::new(Mutex::new(Some(rx_in)));
        let rx_out = Arc::new(Mutex::new(Some(rx_out)));
        let ticker_rx = Arc::new(Mutex::new(Some(ticker_rx)));

        // Clone senders for DstNode before moving originals into the closure.
        let tx_in_for_dst = tx_in.clone();
        let ticker_tx_for_dst = ticker_tx.clone();

        let node_id = spec.node_id.clone();
        let node_id_host = node_id.clone();
        let cluster_port = spec.cluster_port;
        let cluster_ports_host = cluster_ports.clone();

        // Register a named virtual host in the turmoil simulation.
        // The closure is called once per host startup
        // The async body runs inside sim.run(), at which point all hosts
        // are already registered and turmoil::lookup is safe to call.
        sim.host(format!("node-{}", i), move || {
            let rx_in = rx_in.lock().unwrap().take().unwrap();
            let rx_out = rx_out.lock().unwrap().take().unwrap();
            let ticker_rx = ticker_rx.lock().unwrap().take().unwrap();

            let tx_in_for_transport = tx_in.clone();
            let tx_in_for_scheduler = tx_in.clone();
            let ticker_tx_for_actor = ticker_tx.clone();
            let tx_out_for_actor = tx_out.clone();
            let node_id = node_id_host.clone();
            let cluster_ports = cluster_ports_host.clone();

            async move {
                let seed_addrs: Vec<SocketAddr> = (0..n)
                    .filter(|&j| j != i)
                    .map(|j| {
                        SocketAddr::new(turmoil::lookup(format!("node-{}", j)), cluster_ports[j])
                    })
                    .collect();

                let join_config = JoinConfig {
                    seed_addrs,
                    initial_delay_ticks: 1,
                    interval_ticks: 1,
                    multiplier: 2,
                    max_attempts: 5,
                };

                // 0.0.0.0 binds to all interfaces on the virtual host.
                // Turmoil routes packets to this host by its registered name.
                let socket = UdpSocket::bind(("0.0.0.0", cluster_port)).await?;
                let local_addr = socket.local_addr()?;

                let transport =
                    SwimTransportActor::new(socket, tx_in_for_transport, rx_out).await?;
                let actor = SwimActor::new(
                    i as u64,
                    local_addr,
                    node_id,
                    rx_in,
                    tx_out_for_actor,
                    ticker_tx_for_actor,
                    256,
                    join_config,
                );

                tokio::spawn(run_scheduling_actor(tx_in_for_scheduler, ticker_rx));
                tokio::spawn(transport.run());
                actor.run().await;
                Ok(())
            }
        });

        dst_nodes.push(DstNode {
            node_id,
            cluster_addr: format!("{}:{}", spec.host, spec.cluster_port)
                .parse()
                .unwrap(),
            port: spec.port,
            tx_in: tx_in_for_dst,
            ticker_tx: ticker_tx_for_dst,
        });
    }

    dst_nodes
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedulers::ticker::PROBE_INTERVAL_TICKS;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::util::SubscriberInitExt;

    // ---------------------------------------------------------------
    // Tracing capture
    // ---------------------------------------------------------------

    /// A writer that appends all tracing output into a shared byte buffer.
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> { Ok(()) }
    }

    impl<'a> MakeWriter<'a> for SharedWriter {
        type Writer = SharedWriter;
        fn make_writer(&'a self) -> Self::Writer {
            SharedWriter(Arc::clone(&self.0))
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    fn three_node_specs() -> Vec<NodeSpec> {
        vec![
            NodeSpec { node_id: "node-0".into(), host: "127.0.0.1", port: 5000, cluster_port: 15000 },
            NodeSpec { node_id: "node-1".into(), host: "127.0.0.1", port: 5001, cluster_port: 15001 },
            NodeSpec { node_id: "node-2".into(), host: "127.0.0.1", port: 5002, cluster_port: 15002 },
        ]
    }

    /// Runs the simulation once and returns all tracing log lines emitted during the run.
    fn run_once() -> Vec<String> {
        let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        // set_default() installs the subscriber for this thread only and returns a
        // guard — when the guard drops, the previous subscriber is restored.
        let _guard = tracing_subscriber::fmt()
            .with_writer(SharedWriter(Arc::clone(&buf)))
            .without_time() // remove all the wall-clock timestamp
            .with_ansi(false) // remove ANSI color codes
            .set_default();

        let mut sim = turmoil::Builder::new().build();
        let nodes = set_up_cluster(&mut sim, &three_node_specs());

        sim.client("observer", async move {
            // Let actors initialize and fire their first join pings.
            tokio::time::sleep(Duration::from_millis(10)).await;
            for _ in 0..PROBE_INTERVAL_TICKS * 100 {
                for node in &nodes { node.force_tick().await; }
            }
            Ok(())
        });

        sim.run().unwrap();
        drop(_guard); // release the subscriber's Arc clone before unwrapping

        let bytes = Arc::try_unwrap(buf).unwrap().into_inner().unwrap();
        String::from_utf8(bytes).unwrap()
            .lines()
            .map(String::from)
            .collect()
    }


    #[test]
    fn test_simulation_is_deterministic() {
        let logs_first_run = run_once();
        let logs_second_run = run_once();

        let first_len = logs_first_run.len();
        let second_len = logs_second_run.len();

        for i in 0..first_len.min(second_len) {
            assert_eq!(
                logs_first_run[i], logs_second_run[i],
                "log line {i} differs between runs:\n  run1: {}\n  run2: {}",
                logs_first_run[i], logs_second_run[i],
            );
        }

        assert_eq!(
            first_len, second_len,
            "log line counts differ: run1={first_len}, run2={second_len}",
        );
    }
}