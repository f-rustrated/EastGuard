use std::sync::LazyLock;

use std::fs::{self, OpenOptions};
use std::io::Write;

use clap::Parser;
use uuid::Uuid;
pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);

#[derive(Parser, Debug)]
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

    #[arg(long, env = "EASTGUARD_NODE_ID")]
    pub node_id: Option<String>,

    #[arg(long, env = "EASTGUARD_NODE_ID_FILE_NAME", default_value = "node_id")]
    pub node_id_file_name: String,

    #[arg(short, long, env = "EASTGUARD_PORT", default_value_t = 2921)]
    pub port: u16,

    #[arg(long, env = "EASTGUARD_CLUSTER_PORT", default_value_t = 2922)]
    pub cluster_port: u16,

    #[arg(long, env = "EASTGUARD_HOST", default_value = "127.0.0.1")]
    pub host: String,

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
    pub join_max_attempts: u32
}

impl Environment {
    pub fn init() -> Self {
        // 1. Parse arguments from CLI and/or ENV vars
        let env = Environment::parse();

        // 2. Perform side effects (creation/validation) just like before
        for dir in [&env.data_dir, &env.config_dir] {
            if let Err(e) = fs::create_dir_all(dir) {
                eprintln!("Warning: Could not create directory '{}': {}", dir, e);
            }
        }

        // Validate write permissions
        let test_file = format!("{}/.write_test", env.data_dir);
        if let Ok(_) = OpenOptions::new().write(true).create(true).open(&test_file) {
            let _ = fs::remove_file(test_file);
        } else {
            eprintln!(
                "Warning: Server directory '{}' may not be writable.",
                env.data_dir
            );
        }

        env
    }
    pub(crate) fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub(crate) fn peer_bind_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.host, self.cluster_port)
            .parse()
            .expect("Invalid peer bind address")
    }

    /// Returns the node ID, resolving it from (in priority order):
    /// 1. The `--node-id` flag / `EASTGUARD_NODE_ID` env var (if set)
    /// 2. The file at `{config_dir}/{node_id_file_name}` (if it exists and is non-empty)
    /// 3. A freshly generated UUID v4, which is then persisted to that file
    pub fn resolve_node_id(&self) -> String {
        if let Some(id) = &self.node_id {
            return id.clone();
        }

        let path = format!("{}/{}", self.config_dir, self.node_id_file_name);

        if let Ok(contents) = fs::read_to_string(&path) {
            let id = contents.trim();
            if !id.is_empty() {
                return id.to_owned();
            }
        }

        self.generate_and_persist_node_id(&path)
    }

    fn generate_and_persist_node_id(&self, path: &str) -> String {
        let new_id = Uuid::new_v4().to_string();

        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                if let Err(e) = file
                    .write_all(new_id.as_bytes())
                    .and_then(|_| file.sync_all())
                {
                    eprintln!("Warning: failed to persist node_id '{}': {}", path, e);
                }
                new_id
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Another process won — read the canonical ID
                fs::read_to_string(path)
                    .ok()
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .unwrap_or(new_id)
            }
            Err(e) => {
                eprintln!("Warning: failed to create node_id file '{}': {}", path, e);
                new_id
            }
        }
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
            node_id: None,
            node_id_file_name: "node_id".into(),
            port: 2921,
            cluster_port: 2922,
            host: "127.0.0.1".into(),
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
            port: 3000,
            cluster_port: 3001,
            node_id: Some("node-1".into()),
            ..make_env()
        };

        assert_eq!(env.bind_addr(), "127.0.0.1:3000");
        assert_eq!(env.peer_bind_addr(), "127.0.0.1:3001".parse().unwrap());
    }

    #[test]
    fn test_peer_socket_addr() {
        let env = Environment {
            port: 3000,
            ..make_env()
        };
        let addr = env.peer_bind_addr();
        assert_eq!(addr.to_string(), "127.0.0.1:13000");
    }

    #[test]
    #[serial]
    fn test_defaults() {
        let args = vec!["my-server"];

        let env = Environment::try_parse_from(args).expect("Failed to parse defaults");

        assert_eq!(env.config_dir, "./eastguard/config");
        assert_eq!(env.data_dir, "./eastguard/data");
        assert_eq!(env.node_id, None);
        assert_eq!(env.node_id_file_name, "node_id");
        assert_eq!(env.port, 2921);
        assert_eq!(env.cluster_port, 2922);
        assert_eq!(env.host, "127.0.0.1");
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
            "--node-id",
            "node-1",
            "--port",
            "9999",
            "--host",
            "0.0.0.0",
            "--data-dir",
            "/tmp/test",
            "--vnodes-per-node",
            "8",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.node_id, Some("node-1".into()));
        assert_eq!(env.port, 9999);
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

        assert_eq!(env.port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.data_dir, "/tmp/test");
    }

    #[test]
    fn test_join_flags() {
        let args = vec![
            "my-server",
            "--join-seed-nodes",
            "10.0.0.1:2921",
            "--join-seed-nodes",
            "10.0.0.2:2921",
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
            vec!["10.0.0.1:2921".to_string(), "10.0.0.2:2921".to_string()]
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
            std::env::set_var("EASTGUARD_NODE_ID", "env-node-1");
            std::env::set_var("EASTGUARD_PORT", "8888");
            std::env::set_var("EASTGUARD_HOST", "0.0.0.0");
        }

        let env = Environment::try_parse_from(vec!["my-server"]).expect("Failed to parse env vars");

        assert_eq!(env.node_id, Some("env-node-1".into()));
        assert_eq!(env.port, 8888);
        assert_eq!(env.host, "0.0.0.0");

        unsafe {
            std::env::remove_var("EASTGUARD_NODE_ID");
            std::env::remove_var("EASTGUARD_PORT");
            std::env::remove_var("EASTGUARD_HOST");
        }
    }

    #[test]
    fn test_invalid_port_input() {
        let args = vec!["my-server", "--port", "not-a-number"];

        // This should fail because clap validates u16 automatically
        let result = Environment::try_parse_from(args);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_node_id_from_flag() {
        let env = Environment {
            node_id: Some("explicit-node".into()),
            ..make_env()
        };

        assert_eq!(env.resolve_node_id(), "explicit-node");
    }

    #[test]
    fn test_resolve_node_id_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("node_id");
        fs::write(&file_path, "file-node-42").unwrap();

        let env = Environment {
            config_dir: dir.path().to_str().unwrap().into(),
            ..make_env()
        };

        assert_eq!(env.resolve_node_id(), "file-node-42");
    }

    #[test]
    fn test_resolve_node_id_empty_file_generates_new() {
        let dir = tempfile::tempdir().unwrap();
        // Write an empty file — should be treated as absent.
        fs::write(dir.path().join("node_id"), "").unwrap();

        let env = Environment {
            config_dir: dir.path().to_str().unwrap().into(),
            ..make_env()
        };

        let id = env.resolve_node_id();
        assert!(Uuid::parse_str(&id).is_ok());
    }

    #[test]
    fn test_resolve_node_id_generates_and_persists() {
        let dir = tempfile::tempdir().unwrap();

        let env = Environment {
            config_dir: dir.path().to_str().unwrap().into(),
            ..make_env()
        };

        let id1 = env.resolve_node_id();
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert!(Uuid::parse_str(&id1).is_ok());

        // Second call must return the same persisted value
        let id2 = env.resolve_node_id();
        assert_eq!(id1, id2);

        // File must contain the ID
        let on_disk = fs::read_to_string(dir.path().join("node_id")).unwrap();
        assert_eq!(on_disk.trim(), id1);
    }

    #[test]
    fn test_resolve_node_id_persisted_across_restarts() {
        let dir = tempfile::tempdir().unwrap();

        // Simulate first startup: no node_id passed, UUID is generated and persisted.
        let id_first_run = Environment {
            config_dir: dir.path().to_str().unwrap().into(),
            ..make_env()
        }
        .resolve_node_id();

        assert!(Uuid::parse_str(&id_first_run).is_ok());

        // Simulate restart: a fresh Environment pointing to the same config_dir,
        // still without an explicit node_id. It must recover the persisted UUID.
        let id_after_restart = Environment {
            config_dir: dir.path().to_str().unwrap().into(),
            ..make_env()
        }
        .resolve_node_id();

        assert_eq!(id_first_run, id_after_restart);
    }
}
