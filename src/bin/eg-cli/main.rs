use anyhow::Context;
use clap::Parser;
use east_guard::client::{Client, RetryPolicy};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustyline::Editor;
use rustyline::error::ReadlineError;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

mod commands;
mod helper;

use commands::{CliCommand, Commands, execute_command};
use helper::CliHelper;

#[derive(Parser, Debug)]
#[command(name = "eg-cli", about = "EastGuard Interactive CLI")]
struct StartupCli {
    /// Comma-separated list of bootstrap seed addresses (e.g., 127.0.0.1:2921)
    #[arg(short, long, default_value = "127.0.0.1:2921")]
    seeds: String,
    /// Client certificate chain in PEM format (enables mutual TLS)
    #[arg(long, requires_all = ["private_key_path", "trust_root_path"])]
    certificate_chain_path: Option<PathBuf>,
    /// Client private key in PEM format
    #[arg(long, requires_all = ["certificate_chain_path", "trust_root_path"])]
    private_key_path: Option<PathBuf>,
    /// Trusted broker CA certificates in PEM format
    #[arg(long, requires_all = ["certificate_chain_path", "private_key_path"])]
    trust_root_path: Option<PathBuf>,
}

impl StartupCli {
    fn connect(&self, seeds: Vec<SocketAddr>) -> anyhow::Result<Client> {
        match (
            &self.certificate_chain_path,
            &self.private_key_path,
            &self.trust_root_path,
        ) {
            (None, None, None) => Ok(Client::connect(seeds)?),
            (Some(chain), Some(key), Some(ca)) => {
                let certificates = CertificateDer::pem_file_iter(chain)
                    .with_context(|| format!("cannot open certificate chain {}", chain.display()))?
                    .collect::<Result<Vec<_>, _>>()
                    .context("invalid client certificate chain")?;
                let private_key = PrivateKeyDer::from_pem_file(key)
                    .with_context(|| format!("invalid private key file {}", key.display()))?;
                let mut roots = rustls::RootCertStore::empty();
                for certificate in CertificateDer::pem_file_iter(ca)
                    .with_context(|| format!("cannot open trust roots {}", ca.display()))?
                {
                    roots
                        .add(certificate?)
                        .context("invalid trust root certificate")?;
                }
                anyhow::ensure!(
                    !roots.is_empty(),
                    "trust root file contains no certificates"
                );
                let tls = rustls::ClientConfig::builder_with_protocol_versions(&[
                    &rustls::version::TLS13,
                ])
                .with_root_certificates(roots)
                .with_client_auth_cert(certificates, private_key)?;
                Ok(Client::connect_secure(
                    seeds,
                    RetryPolicy::default(),
                    Arc::new(tls),
                )?)
            }
            _ => anyhow::bail!(
                "mutual TLS requires certificate chain, private key, and trust root paths"
            ),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Only log warnings and above so the REPL isn't cluttered
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();
    let startup = StartupCli::parse();

    let seeds: Vec<SocketAddr> = startup
        .seeds
        .split(',')
        .map(str::parse)
        .collect::<Result<_, _>>()
        .context("invalid socket address in seeds")?;

    let ascii_art = r#"
  ______           _    _____                     _ 
 |  ____|         | |  / ____|                   | |
 | |__   __ _ ___ | |_| |  __ _   _  __ _ _ __ __| |
 |  __| / _` / __|| __| | |_ | | | |/ _` | '__/ _` |
 | |___| (_| \__ \| |_| |__| | |_| | (_| | | | (_| |
 |______\__,_|___/ \__|\_____|\__,_|\__,_|_|  \__,_|
"#;
    println!("\x1b[1;36m{}\x1b[0m", ascii_art);
    println!("Connecting to EastGuard cluster at {:?}...", seeds);
    let client = Arc::new(startup.connect(seeds)?);
    println!("Connected! Type 'help' for available commands.");

    let mut rl = Editor::new()?;
    rl.set_helper(Some(CliHelper));

    loop {
        // \x1b[1;32m = bold green, \x1b[0m = reset
        let readline = rl.readline("\x1b[1;32meastguard>\x1b[0m ");
        match readline {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line.as_str());

                let mut args = match shlex::split(&line) {
                    Some(args) => args,
                    None => {
                        println!("Error: mismatched quotes");
                        continue;
                    }
                };

                // clap requires the first argument to be the program name
                args.insert(0, "".to_string());

                match CliCommand::try_parse_from(args) {
                    Ok(cli) => {
                        if matches!(cli.command, Commands::Exit | Commands::Quit) {
                            break;
                        } else if let Err(e) = execute_command(cli.command, &client).await {
                            println!("Error: {}", e);
                        }
                    }
                    Err(e) => {
                        // Print help or parsing error
                        println!("{}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    println!("Goodbye!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn certificate_options_require_all_three_files() {
        for present in 0..8 {
            let mut args = vec!["eg-cli"];
            for (bit, flag) in [
                "--certificate-chain-path",
                "--private-key-path",
                "--trust-root-path",
            ]
            .into_iter()
            .enumerate()
            {
                if present & (1 << bit) != 0 {
                    args.extend([flag, "missing.pem"]);
                }
            }
            let parsed = StartupCli::try_parse_from(args);
            assert_eq!(parsed.is_ok(), present == 0 || present == 7);
            if present == 7 {
                assert!(
                    parsed
                        .unwrap()
                        .connect(vec!["127.0.0.1:2921".parse().unwrap()])
                        .is_err()
                );
            }
        }
    }

    #[tokio::test]
    async fn cli_authenticates_with_its_configured_certificate() -> anyhow::Result<()> {
        use east_guard::client::{ClientSuccess, PartitionStrategy, StoragePolicy};
        use rcgen::{CertificateParams, KeyPair, SanType, string::Ia5String};
        use std::io::{Read, Write};
        use std::time::Duration;

        let files = tempfile::tempdir()?;
        let certificate = |uri: &str| -> anyhow::Result<_> {
            let key = KeyPair::generate()?;
            let mut params = CertificateParams::default();
            params.subject_alt_names = vec![
                SanType::URI(Ia5String::try_from(uri)?),
                SanType::IpAddress("127.0.0.1".parse()?),
            ];
            Ok((params.self_signed(&key)?, key))
        };
        let (client_cert, client_key) = certificate("urn:eastguard:client:operator")?;
        let (server_cert, server_key) = certificate("urn:eastguard:node:broker")?;
        for (file, kind, bytes) in [
            ("client.pem", "CERTIFICATE", client_cert.der().as_ref()),
            ("client.key", "PRIVATE KEY", client_key.serialized_der()),
            ("ca.pem", "CERTIFICATE", server_cert.der().as_ref()),
        ] {
            std::fs::write(
                files.path().join(file),
                format!(
                    "-----BEGIN {kind}-----\n{}\n-----END {kind}-----\n",
                    data_encoding::BASE64.encode(bytes),
                ),
            )?;
        }
        let args = StartupCli::try_parse_from([
            "eg-cli".as_ref(),
            "--certificate-chain-path".as_ref(),
            files.path().join("client.pem").as_os_str(),
            "--private-key-path".as_ref(),
            files.path().join("client.key").as_os_str(),
            "--trust-root-path".as_ref(),
            files.path().join("ca.pem").as_os_str(),
        ])?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(client_cert.der().clone())?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        let server =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    vec![server_cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
                )?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let client = args.connect(vec![listener.local_addr()?])?;
        let server = std::thread::spawn(move || -> anyhow::Result<()> {
            let (socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(3)))?;
            socket.set_write_timeout(Some(Duration::from_secs(3)))?;
            let connection = rustls::ServerConnection::new(Arc::new(server))?;
            let mut stream = rustls::StreamOwned::new(connection, socket);
            let mut length = [0; 4];
            stream.read_exact(&mut length)?;
            assert_eq!(
                stream.conn.peer_certificates().unwrap(),
                &[client_cert.der().clone()]
            );
            let length = u32::from_be_bytes(length) as usize;
            assert!((10..1024).contains(&length));
            let mut request = vec![0; length];
            stream.read_exact(&mut request)?;
            // Reply envelope: Ok tag followed by the SDK's success payload.
            let mut response = vec![0];
            response.extend(borsh::to_vec(&ClientSuccess::TopicCreated)?);
            stream.write_all(&((8 + response.len()) as u32).to_be_bytes())?;
            stream.write_all(&request[..8])?;
            stream.write_all(&response)?;
            stream.flush()?;
            Ok(())
        });
        let created = tokio::time::timeout(
            Duration::from_secs(5),
            client.create_topic(
                "orders",
                StoragePolicy {
                    retention_ms: None,
                    replication_factor: 1,
                    partition_strategy: PartitionStrategy::Fixed,
                },
            ),
        )
        .await??;
        assert!(created);
        server.join().unwrap()?;

        // Invalid credentials must fail construction instead of selecting plaintext.
        for path in [
            &args.trust_root_path,
            &args.certificate_chain_path,
            &args.private_key_path,
        ] {
            let path = path.as_ref().unwrap();
            let original = std::fs::read(path)?;
            std::fs::write(path, b"not a PEM file")?;
            assert!(args.connect(vec!["127.0.0.1:2921".parse()?]).is_err());
            std::fs::write(path, original)?;
        }
        Ok(())
    }
}
