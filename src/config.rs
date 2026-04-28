use std::sync::LazyLock;

use std::fs::{self, OpenOptions};

use clap::Parser;
use uuid::Uuid;

use crate::clusters::{NodeAddress, NodeId};
use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::clusters::swims::swim::Swim;
use crate::clusters::swims::{Topology, TopologyConfig};
use crate::schedulers::actor::TICK_PERIOD_MS;
pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Environment {
    #[arg(
        long,
        env = "EASTGUARD_CONFIG_DIR",
        default_value = "./eastguard/config"
    )]
    pub config_dir: String,

    #[arg(long, env = "EASTGUARD_DATA_DIR", default_value = "./eastguard/data")]
    pub data_dir: String,

    #[arg(long, env = "EASTGUARD_META_DIR", default_value = "./eastguard/meta")]
    pub meta_dir: String,

    #[arg(long = "node-id-prefix", env = "EASTGUARD_NODE_ID_PREFIX")]
    pub node_id_prefix: Option<String>,

    #[arg(short = 'p', long = "client-port", env = "EASTGUARD_CLIENT_PORT", default_value_t = 2921)]
    pub client_port: u16,

    #[arg(long, env = "EASTGUARD_CLUSTER_PORT", default_value_t = 2922)]
    pub cluster_port: u16,

    #[arg(long, env = "EASTGUARD_HOST", default_value = "0.0.0.0")]
    pub host: String,

    /// The IP address gossiped to cluster peers so they can reach this node.
    ///
    /// # Why this exists
    ///
    /// A node typically binds its sockets to `0.0.0.0` (all interfaces) so it accepts
    /// traffic regardless of which NIC a packet arrives on. That is correct for *listening*,
    /// but it is wrong for *advertising*: `0.0.0.0` is not routable. If a node gossips
    /// `0.0.0.0` as its address, peers will store it and send probes there — probes that
    /// can never be delivered, causing false suspicion and eventual eviction from the cluster.
    ///
    /// `host` controls where the sockets bind; `advertise_host` controls what address is
    /// published to the rest of the cluster. The two concerns are independent:
    ///
    /// - In a single-NIC VM you may set both to the same value.
    /// - Behind a NAT or load balancer you bind to a private IP but advertise the public one.
    /// - In a container you bind to `0.0.0.0` but advertise the pod/container IP.
    ///
    /// When `advertise_host` is `None`, it falls back to `host`. This is fine in development
    /// when `host` is already a routable address, but in any multi-node environment you should
    /// set it explicitly.
    #[arg(long, env = "EASTGUARD_ADVERTISE_HOST")]
    pub advertise_host: Option<String>,

    #[arg(long, env = "EASTGUARD_VNODES_PER_NODE", default_value_t = 256)]
    pub vnodes_per_node: u64,

    #[arg(long, env = "EASTGUARD_JOIN_SEED_NODES")]
    pub join_seed_nodes: Vec<String>,

    // TODO: We know that when join_initial_delay_ms < tick interval, join process will fire immediately. Let's fix this when we think we need to.
    #[arg(long, env = "EASTGUARD_JOIN_INITIAL_DELAY_MS", default_value_t = 1000)]
    pub join_initial_delay_ms: u64,

    #[arg(long, env = "EASTGUARD_JOIN_INTERVAL_MS", default_value_t = 1000)]
    pub join_interval_ms: u64,

    #[arg(long, env = "EASTGUARD_JOIN_MULTIPLIER", default_value_t = 2)]
    pub join_multiplier: u32,

    #[arg(long, env = "EASTGUARD_JOIN_MAX_ATTEMPTS", default_value_t = 5)]
    pub join_max_attempts: u32,
}

impl Environment {
    pub fn init() -> Self {
        // 1. Parse arguments from CLI and/or ENV vars
        let env = Environment::parse();

        if env.client_port == env.cluster_port {
            panic!(
                "client_port ({}) and cluster_port ({}) must be different",
                env.client_port, env.cluster_port
            );
        }

        // 2. Ensure data and meta directories exist and are writable
        for dir in [&env.data_dir, &env.meta_dir] {
            if let Err(e) = fs::create_dir_all(dir) {
                panic!("Warning: Could not create directory '{}': {}", dir, e);
            }
            let test_file = format!("{}/.write_test", dir);
            if OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&test_file)
                .is_ok()
            {
                let _ = fs::remove_file(test_file);
            } else {
                panic!("Warning: Server directory '{}' may not be writable.", dir);
            }
        }

        env
    }
    pub(crate) fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.client_port)
    }

    pub(crate) fn raft_db_path(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.meta_dir).join("raft")
    }

    pub(crate) fn peer_bind_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.host, self.cluster_port)
            .parse()
            .expect("Invalid peer bind address")
    }

    /// The address gossiped to cluster peers — must be routable by other nodes.
    /// Defaults to `host:cluster_port` when `advertise_host` is not set.
    ///
    /// # Panics
    ///
    /// Panics if the resulting advertise address is `0.0.0.0`, which is non-routable
    /// and would cause peers to send probes to an unreachable destination.
    pub(crate) fn advertise_peer_addr(&self) -> std::net::SocketAddr {
        let host = self.advertise_host.as_deref().unwrap_or(&self.host);
        let addr: std::net::SocketAddr = format!("{}:{}", host, self.cluster_port)
            .parse()
            .expect("Invalid advertise peer address");

        if addr.ip().is_unspecified() {
            panic!(
                "Advertise address {} is non-routable. \
                 Set --advertise-host to a routable IP (e.g. 127.0.0.1 for local dev, \
                 or the node's real IP for production).",
                addr
            );
        }

        addr
    }

    /// Returns the node identity for this process start.
    ///
    /// - With `--node-id-prefix`: returns `{prefix}::{uuid}` — the prefix lets operators
    ///   identify the physical node; the UUID ensures each start is a distinct identity so
    ///   a restarted node is never confused with its previously-dead incarnation.
    /// - Without a prefix: returns a bare UUID.
    pub fn resolve_node_id(&self) -> String {
        match &self.node_id_prefix {
            Some(prefix) => format!("{}::{}", prefix, Uuid::new_v4()),
            None => Uuid::new_v4().to_string(),
        }
    }

    pub(crate) fn bootstrap_servers(&self) -> Vec<JoinAttempt> {
        self.join_seed_nodes
            .iter()
            .filter_map(|s| s.parse().ok())
            .map(|addr| JoinAttempt {
                seed_addr: addr,
                ticks_for_wait: (self.join_initial_delay_ms / TICK_PERIOD_MS) as u32,
                backoff_ticks: (self.join_interval_ms / TICK_PERIOD_MS) as u32,
                multiplier: self.join_multiplier,
                max_attempts: self.join_max_attempts,
                remaining_attempts: self.join_max_attempts,
            })
            .collect()
    }

    pub(crate) fn advertise_client_addr(&self) -> std::net::SocketAddr {
        let host = self.advertise_host.as_deref().unwrap_or(&self.host);
        format!("{}:{}", host, self.client_port)
            .parse()
            .expect("Invalid advertise client address")
    }

    pub(crate) fn swim(&self, rng_seed: u64) -> Swim {
        Swim::new(
            NodeId::new(self.resolve_node_id()),
            NodeAddress {
                cluster_addr: self.advertise_peer_addr(),
                client_addr: self.advertise_client_addr(),
            },
            Topology::new(
                std::iter::empty(),
                TopologyConfig {
                    vnodes_per_pnode: self.vnodes_per_node,
                    replication_factor: 3,
                },
            ),
            rng_seed,
        )
        .bootstrap(self.bootstrap_servers())
    }
}

pub const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serial_test::serial;

    fn make_env() -> Environment {
        Environment {
            config_dir: "./eastguard/config".into(),
            data_dir: "./eastguard/data".into(),
            meta_dir: "./eastguard/meta".into(),
            node_id_prefix: None,
            client_port: 2921,
            cluster_port: 2922,
            host: "127.0.0.1".into(),
            advertise_host: None,
            vnodes_per_node: 256,
            join_seed_nodes: vec![],
            join_initial_delay_ms: 1000,
            join_interval_ms: 1000,
            join_multiplier: 2,
            join_max_attempts: 5,
        }
    }

    #[test]
    fn test_address_formatting() {
        let env = Environment {
            client_port: 3000,
            cluster_port: 3001,
            node_id_prefix: Some("node-1".into()),
            ..make_env()
        };

        assert_eq!(env.bind_addr(), "127.0.0.1:3000");
        assert_eq!(env.peer_bind_addr(), "127.0.0.1:3001".parse().unwrap());
    }

    #[test]
    #[serial]
    fn test_defaults() {
        let args = vec!["my-server"];

        let env = Environment::try_parse_from(args).expect("Failed to parse defaults");

        assert_eq!(env.config_dir, "./eastguard/config");
        assert_eq!(env.data_dir, "./eastguard/data");
        assert_eq!(env.meta_dir, "./eastguard/meta");
        assert_eq!(env.node_id_prefix, None);
        assert_eq!(env.client_port, 2921);
        assert_eq!(env.cluster_port, 2922);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.vnodes_per_node, 256);
        assert_eq!(env.join_seed_nodes, Vec::<String>::new());
        assert_eq!(env.join_initial_delay_ms, 1000);
        assert_eq!(env.join_interval_ms, 1000);
        assert_eq!(env.join_multiplier, 2);
        assert_eq!(env.join_max_attempts, 5);
    }

    #[test]
    fn test_flags_override() {
        let args = vec![
            "my-server",
            "--node-id-prefix",
            "node-1",
            "--client-port",
            "9999",
            "--host",
            "0.0.0.0",
            "--data-dir",
            "/tmp/test",
            "--vnodes-per-node",
            "8",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.node_id_prefix, Some("node-1".into()));
        assert_eq!(env.client_port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.data_dir, "/tmp/test");
        assert_eq!(env.vnodes_per_node, 8);
    }

    #[test]
    fn test_flags_override2() {
        let args = vec![
            "my-server",
            "-p",
            "9999",
            "--host",
            "0.0.0.0", // -h is preserved for --help.
            "--data-dir",
            "/tmp/test",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.client_port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.data_dir, "/tmp/test");
    }

    #[test]
    fn test_join_flags() {
        let args = vec![
            "my-server",
            "--join-seed-nodes",
            "10.0.0.1:2922",
            "--join-seed-nodes",
            "10.0.0.2:2922",
            "--join-initial-delay-ms",
            "500",
            "--join-interval-ms",
            "750",
            "--join-multiplier",
            "3",
            "--join-max-attempts",
            "10",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse join flags");

        assert_eq!(
            env.join_seed_nodes,
            vec!["10.0.0.1:2922".to_string(), "10.0.0.2:2922".to_string()]
        );
        assert_eq!(env.join_initial_delay_ms, 500);
        assert_eq!(env.join_interval_ms, 750);
        assert_eq!(env.join_multiplier, 3);
        assert_eq!(env.join_max_attempts, 10);
    }

    #[test]
    #[serial]
    fn test_env_vars_override() {
        unsafe {
            std::env::set_var("EASTGUARD_NODE_ID_PREFIX", "env-node-1");
            std::env::set_var("EASTGUARD_CLIENT_PORT", "8888");
            std::env::set_var("EASTGUARD_HOST", "0.0.0.0");
        }

        let env = Environment::try_parse_from(vec!["my-server"]).expect("Failed to parse env vars");

        assert_eq!(env.node_id_prefix, Some("env-node-1".into()));
        assert_eq!(env.client_port, 8888);
        assert_eq!(env.host, "0.0.0.0");

        unsafe {
            std::env::remove_var("EASTGUARD_NODE_ID_PREFIX");
            std::env::remove_var("EASTGUARD_CLIENT_PORT");
            std::env::remove_var("EASTGUARD_HOST");
        }
    }

    #[test]
    fn test_invalid_port_input() {
        let args = vec!["my-server", "--client-port", "not-a-number"];

        // This should fail because clap validates u16 automatically
        let result = Environment::try_parse_from(args);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_node_id_with_prefix_appends_uuid() {
        let env = Environment {
            node_id_prefix: Some("my-node".into()),
            ..make_env()
        };
        let id1 = env.resolve_node_id();
        let id2 = env.resolve_node_id();
        // Each call produces a different identity.
        assert_ne!(id1, id2);
        // Both carry the operator-configured prefix.
        assert!(id1.starts_with("my-node::"));
        assert!(id2.starts_with("my-node::"));
    }

    #[test]
    fn resolve_node_id_without_prefix_returns_uuid() {
        let env = make_env(); // node_id_prefix: None
        let id = env.resolve_node_id();
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn swim_node_id_matches_resolve_node_id_format() {
        let env = Environment {
            node_id_prefix: Some("my-node".into()),
            ..make_env()
        };
        let id1 = env.swim(0).node_id;
        let id2 = env.swim(0).node_id;
        assert_ne!(*id1, *id2);
        assert!(id1.starts_with("my-node::"));
        assert!(id2.starts_with("my-node::"));
    }
}
