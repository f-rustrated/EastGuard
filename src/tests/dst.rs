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
                // local_addr() would return 0.0.0.0 (wildcard), but the SWIM
                // actor gossips this address to peers who then can't probe back.
                // Use turmoil::lookup to get the actual simulated IP instead.
                let socket = UdpSocket::bind(("0.0.0.0", cluster_port)).await?;
                let local_addr = SocketAddr::new(
                    turmoil::lookup(format!("node-{}", i)),
                    cluster_port,
                );

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

#[cfg(madsim)]
mod tests {
    use super::*;
    use crate::schedulers::actor::TICK_PERIOD_MS;
    use crate::schedulers::ticker::PROBE_INTERVAL_TICKS;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::util::SubscriberInitExt;

    // ---------------------------------------------------------------
    // Mad-turmoil: seed all randomness (HashMap seeds, getrandom, …)
    // and override clock_gettime with turmoil's virtual clock.
    //
    // Activated only when compiled with RUSTFLAGS="--cfg madsim".
    // Entirely inside #[cfg(test)] — never compiled into production.
    //
    // Run DST tests in isolation to avoid clock-override interference
    // with the non-turmoil unit tests in clusters/tests.rs:
    //
    //   RUSTFLAGS="--cfg madsim" cargo test -- --test-threads=1
    // ---------------------------------------------------------------
    mod mad {
        use std::sync::Once;

        // mad_turmoil::rand::set_rng uses OnceLock internally, so it can
        // only be called once per process.  Wrap with std::sync::Once so
        // multiple test functions in the same binary don't panic.
        static RNG_INIT: Once = Once::new();

        /// Seed all randomness and return the clock guard.
        ///
        /// The caller **must** hold the returned guard for the lifetime of
        /// the simulation.  Dropping it resets the clock override so that
        /// non-turmoil tests running afterward are not affected.
        pub fn init(seed: u64) -> mad_turmoil::time::SimClocksGuard {
            RNG_INIT.call_once(|| {
                use rand_sim::SeedableRng;
                mad_turmoil::rand::set_rng(rand_sim::rngs::StdRng::seed_from_u64(seed));
            });
            mad_turmoil::time::SimClocksGuard::init()
        }
    }

    // ---------------------------------------------------------------
    // Log capture
    // ---------------------------------------------------------------

    /// Appends all tracing output into a shared byte buffer.
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
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
            NodeSpec {
                node_id: "node-0".into(),
                host: "127.0.0.1",
                port: 5000,
                cluster_port: 15000,
            },
            NodeSpec {
                node_id: "node-1".into(),
                host: "127.0.0.1",
                port: 5001,
                cluster_port: 15001,
            },
            NodeSpec {
                node_id: "node-2".into(),
                host: "127.0.0.1",
                port: 5002,
                cluster_port: 15002,
            },
        ]
    }

    /// Returns the DST seed.  Set `DST_SEED=<u64>` to reproduce a failure.
    fn dst_seed() -> u64 {
        std::env::var("DST_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(42)
    }

    /// Runs the simulation once with `seed` and returns all tracing log
    /// lines emitted during the run.
    ///
    /// Mad-turmoil must already be initialised by the caller (if needed)
    /// before this function is called.
    fn run_once(seed: u64) -> Vec<String> {
        let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

        // set_default() installs the subscriber for this thread only and
        // returns a guard — dropping it restores the previous subscriber.
        let _sub = tracing_subscriber::fmt()
            .with_writer(SharedWriter(Arc::clone(&buf)))
            .without_time()  // remove wall-clock timestamps
            .with_ansi(false) // remove ANSI colour codes
            .set_default();

        let wait = Duration::from_millis(TICK_PERIOD_MS * PROBE_INTERVAL_TICKS as u64 * 20);
        let sim_duration = wait + Duration::from_secs(5);

        let mut sim = turmoil::Builder::new()
            .rng_seed(seed)
            .tick_duration(Duration::from_millis(10))
            .simulation_duration(sim_duration)
            .build();

        let _nodes = set_up_cluster(&mut sim, &three_node_specs());

        sim.client("observer", async move {
            tokio::time::sleep(wait).await;
            Ok(())
        });

        sim.run().unwrap();

        // Drop the subscriber guard before unwrapping the Arc so that the
        // subscriber's own SharedWriter clone is released first.
        drop(_sub);

        let bytes = Arc::try_unwrap(buf).unwrap().into_inner().unwrap();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    // ---------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------

    /// Verifies that a three-node cluster fully converges: every node must
    /// see all three members in its topology after enough virtual time.
    ///
    /// Without `--cfg madsim` this is a plain convergence test.
    /// With `--cfg madsim` all entropy sources are seeded, making any
    /// failure deterministically reproducible.  Re-run a failing seed:
    ///
    ///   RUSTFLAGS="--cfg madsim" DST_SEED=<seed> \
    ///     cargo test test_cluster_converges -- --test-threads=1
    #[test]
    fn test_cluster_converges() {
        let seed = dst_seed();

        // Activate mad-turmoil for this test only.
        // _mad_guard resets the clock override on drop (end of function),
        // so non-turmoil unit tests running afterward are unaffected.
        let _mad_guard = mad::init(seed);

        // 20 probe rounds is well above what SWIM needs to converge.
        let wait = Duration::from_millis(TICK_PERIOD_MS * PROBE_INTERVAL_TICKS as u64 * 20);
        let sim_duration = wait + Duration::from_secs(5);

        let mut sim = turmoil::Builder::new()
            .rng_seed(seed)
            .tick_duration(Duration::from_millis(10))
            .simulation_duration(sim_duration)
            .build();

        let nodes = set_up_cluster(&mut sim, &three_node_specs());

        sim.client("observer", async move {
            tokio::time::sleep(wait).await;
            // Brief drain so any in-flight UDP packets are delivered.
            tokio::time::sleep(Duration::from_millis(500)).await;

            for node in &nodes {
                let count = node.query_topology_count().await;
                assert_eq!(
                    count,
                    3,
                    "[seed={seed}] node {} sees {count} member(s), expected 3",
                    node.node_id,
                );
            }
            Ok(())
        });

        sim.run().unwrap();
    }

    /// Verifies that the simulation is deterministic: two runs with the
    /// same seed must produce byte-for-byte identical log output.
    ///
    /// With `--cfg madsim`, mad-turmoil seeds all entropy (getrandom /
    /// HashMap SipHash, clock_gettime) before both runs so that every
    /// non-deterministic source within a single process is eliminated.
    ///
    /// The mad-turmoil guard is held across **both** runs intentionally:
    /// the clock override must stay active between them, and releasing it
    /// between runs would re-introduce real-clock noise.
    ///
    /// Run in isolation to prevent RNG-state contamination from earlier
    /// tests in the same binary:
    ///
    ///   RUSTFLAGS="--cfg madsim" cargo test test_simulation_is_deterministic \
    ///     -- --test-threads=1
    #[test]
    fn test_simulation_is_deterministic() {
        let seed = dst_seed();

        // Hold the clock guard across BOTH runs so the virtual-clock
        // override stays active between the two simulations.
        let _mad_guard = mad::init(seed);


        let logs_first = run_once(seed);
        let logs_second = run_once(seed);

        let first_len = logs_first.len();
        let second_len = logs_second.len();

        for i in 0..first_len.min(second_len) {
            assert_eq!(
                logs_first[i], logs_second[i],
                "[seed={seed}] log line {i} differs between runs:\n  run1: {}\n  run2: {}",
                logs_first[i], logs_second[i],
            );
        }

        assert_eq!(
            first_len, second_len,
            "[seed={seed}] log line count differs: run1={first_len}, run2={second_len}",
        );
    }
}
