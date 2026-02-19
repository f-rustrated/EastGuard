use std::sync::LazyLock;

use std::fs::{self, OpenOptions};

use clap::Parser;
pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Environment {
    #[arg(long, env = "CONFIG_DIR", default_value = "./eastguard/config")]
    pub config_dir: String,

    #[arg(long, env = "DATA_DIR", default_value = "./eastguard/data")]
    pub data_dir: String,

    #[arg(long, env = "NODE_ID")]
    pub node_id: Option<String>,

    #[arg(long, env = "NODE_ID_FILE_NAME", default_value = "node_id")]
    pub node_id_file_name: String,

    #[arg(short, long, env = "PORT", default_value_t = 2921)]
    pub port: u16,

    #[arg(long, env = "CLUSTER_PORT", default_value_t = 2922)]
    pub cluster_port: u16,

    #[arg(long, env = "HOST", default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, env = "VNODES_PER_NODE", default_value_t = 256)]
    pub vnodes_per_pnode: u64,
}

impl Environment {
    pub fn init() -> Self {
        // 1. Parse arguments from CLI and/or ENV vars
        let env = Environment::parse();

        // 2. Perform side effects (creation/validation) just like before
        if let Err(e) = fs::create_dir_all(&env.data_dir) {
            eprintln!("Warning: Could not create directory '{}': {}", env.data_dir, e);
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

    pub(crate) fn peer_bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.cluster_port)
    }

    pub(crate) fn peer_socket_addr(&self) -> std::net::SocketAddr {
        format!("{}:{}", self.host, self.port + 10000)
            .parse()
            .expect("Invalid peer bind address")
    }
}

pub const SERDE_CONFIG: bincode::config::Configuration = bincode::config::standard();
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_address_formatting() {
        let env = Environment {
            config_dir: "./eastguard/config".into(),
            data_dir: "./test".into(),
            node_id: Some("node-1".into()),
            node_id_file_name: "node_id".into(),
            port: 3000,
            cluster_port: 3001,
            host: "127.0.0.1".into(),
            vnodes_per_pnode: 256,
        };

        assert_eq!(env.bind_addr(), "127.0.0.1:3000");
        assert_eq!(env.peer_bind_addr(), "127.0.0.1:3001");
    }

    #[test]
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
        assert_eq!(env.vnodes_per_pnode, 256);
    }

    #[test]
    fn test_flags_override() {
        // Simulate: my-server --node-id node-1 --port 9999 --host 0.0.0.0 --data-dir /tmp/test
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
            "--vnodes-per-pnode",
            "8",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.node_id, Some("node-1".into()));
        assert_eq!(env.port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.data_dir, "/tmp/test");
        assert_eq!(env.vnodes_per_pnode, 8);
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
    fn test_invalid_port_input() {
        let args = vec!["my-server", "--port", "not-a-number"];

        // This should fail because clap validates u16 automatically
        let result = Environment::try_parse_from(args);
        assert!(result.is_err());
    }
}
