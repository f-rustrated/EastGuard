use std::sync::LazyLock;

use std::fs::{self, OpenOptions};

use clap::Parser;
pub static ENV: LazyLock<Environment> = LazyLock::new(Environment::init);

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Environment {
    #[arg(short, long, env = "SERVER_DIR", default_value = "./data")]
    pub dir: String,

    /// --port or -p or PORT=
    #[arg(short, long, env = "PORT", default_value_t = 8080)]
    pub port: u16,

    /// --host or -h or HOST=
    #[arg(long, env = "HOST", default_value = "127.0.0.1")]
    pub host: String,
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
        format!("{}:{}", self.host, self.port + 10000)
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
        };

        assert_eq!(env.bind_addr(), "127.0.0.1:3000");
        assert_eq!(env.peer_bind_addr(), "127.0.0.1:13000");
    }

    #[test]
    fn test_defaults() {
        let args = vec!["my-server"];

        let env = Environment::try_parse_from(args).expect("Failed to parse defaults");

        assert_eq!(env.port, 8080); // The default we set
        assert_eq!(env.host, "127.0.0.1");
        assert_eq!(env.dir, "./data");
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
        ];

        let env = Environment::try_parse_from(args).expect("Failed to parse flags");

        assert_eq!(env.port, 9999);
        assert_eq!(env.host, "0.0.0.0");
        assert_eq!(env.dir, "/tmp/test");
    }

    #[test]
    fn test_flags_override2() {
        let args = vec![
            "my-server",
            "-p",
            "9999",
            "--host", // -h is preserved for --help.
            "0.0.0.0",
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
