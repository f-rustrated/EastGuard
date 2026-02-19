use std::sync::LazyLock;

use std::fs::{self, OpenOptions};

use clap::Parser;
pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Environment {
    /// TODO: Need to decide whether to provide a NodeId
    /// #[arg(long, env = "NODE_ID", default_value = "eastguard")]
    /// pub node_id: String,

    #[arg(short, long, env = "SERVER_DIR", default_value = "./data")]
    pub dir: String,

    /// --port or -p or PORT=
    #[arg(short, long, env = "PORT", default_value_t = 2921)]
    pub port: u16,

    #[arg(short, long, env = "CLUSTER_PORT", default_value_t = 2922)]
    pub cluster_port: u16,

    /// --host or -h or HOST=
    #[arg(long, env = "HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Number of virtual nodes (vnodes) placed on the ring per physical node.
    /// Higher values improve key distribution but increase memory usage.
    /// --replicas-per-node or REPLICAS_PER_NODE=
    #[arg(long, env = "REPLICAS_PER_NODE", default_value_t = 256)]
    pub replicas_per_node: u64,
}

impl Environment {
    pub fn init() -> Self {
        // 1. Parse arguments from CLI and/or ENV vars
        let env = Environment::parse();

        // 2. Perform side effects (creation/validation) just like before
        if let Err(e) = fs::create_dir_all(&env.dir) {
            eprintln!("Warning: Could not create directory '{}': {}", env.dir, e);
        }

        // Validate write permissions
        let test_file = format!("{}/.write_test", env.dir);
        if let Ok(_) = OpenOptions::new().write(true).create(true).open(&test_file) {
            let _ = fs::remove_file(test_file);
        } else {
            eprintln!(
                "Warning: Server directory '{}' may not be writable.",
                env.dir
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
            dir: "./test".into(),
            port: 3000,
            host: "127.0.0.1".into(),
            cluster_port: 3001,
            replicas_per_node: 256,
        };

        assert_eq!(env.bind_addr(), "127.0.0.1:3000");
        assert_eq!(env.peer_bind_addr(), "127.0.0.1:3001");
    }

    #[test]
    fn test_defaults() {
        let args = vec!["my-server"];

        let env = Environment::try_parse_from(args).expect("Failed to parse defaults");

        assert_eq!(env.port, 2921); // The default we set
        assert_eq!(env.cluster_port, 2922);
        assert_eq!(env.host, "127.0.0.1");
        assert_eq!(env.dir, "./data");
        assert_eq!(env.replicas_per_node, 256);
    }

    #[test]
    fn test_flags_override() {
        // Simulate: my-server --port 9999 --host 0.0.0.0 --dir /tmp/test
        let args = vec![
            "my-server",
            "--port",
            "9999",
            "--host",
            "0.0.0.0",
            "--dir",
            "/tmp/test",
            "--replicas-per-node",
            "8",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.dir, "/tmp/test");
        assert_eq!(env.replicas_per_node, 8);
    }

    #[test]
    fn test_flags_override2() {
        let args = vec![
            "my-server",
            "-p",
            "9999",
            "--host",
            "0.0.0.0", // -h is preserved for --help.
            "-d",
            "/tmp/test",
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.dir, "/tmp/test");
    }

    #[test]
    fn test_invalid_port_input() {
        let args = vec!["my-server", "--port", "not-a-number"];

        // This should fail because clap validates u16 automatically
        let result = Environment::try_parse_from(args);
        assert!(result.is_err());
    }
}
