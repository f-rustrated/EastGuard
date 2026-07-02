use clap::{Parser, Subcommand};
use east_guard::client::{
    Client, Consumer, ConsumerRecord, KeyInterest, PartitionStrategy, Producer, ProducerConfig,
    StoragePolicy,
};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::{Context, Editor};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "", disable_help_flag = true, disable_version_flag = true)]
struct CliCommand {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create a new topic
    CreateTopic {
        topic: String,
        #[arg(default_value = "3")]
        replication_factor: u64,
    },
    /// Publish a message to a topic
    Publish {
        topic: String,
        key: String,
        message: String,
    },
    /// Consume messages from a topic (up to 'count' records)
    Consume {
        topic: String,
        #[arg(default_value = "earliest")]
        start: String,
        /// 0 means no bound
        #[arg(default_value = "0")]
        count: usize,
        /// Optional timeout in seconds to wait for new messages. If absent, waits indefinitely.
        #[arg(short, long)]
        timeout_sec: Option<u64>,
        /// Optional group ID for consumer group offset management
        #[arg(short, long)]
        group: Option<String>,
    },
    /// Exit the CLI
    Exit,
    Quit,
}

#[derive(Parser, Debug)]
#[command(name = "eg-cli", about = "EastGuard Interactive CLI")]
struct StartupCli {
    /// Comma-separated list of bootstrap seed addresses (e.g., 127.0.0.1:2921)
    #[arg(short, long, default_value = "127.0.0.1:2921")]
    seeds: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Only log warnings and above so the REPL isn't cluttered
    let _ = tracing_subscriber::fmt().with_env_filter("warn").try_init();
    let startup = StartupCli::parse();

    let seeds: Vec<SocketAddr> = startup
        .seeds
        .split(',')
        .map(|s| s.parse().expect("Invalid socket address in seeds"))
        .collect();

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
    let client = match Client::connect(seeds) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
            return Ok(());
        }
    };
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
async fn execute_command(cmd: Commands, client: &Arc<Client>) -> anyhow::Result<()> {
    match cmd {
        Commands::CreateTopic {
            topic,
            replication_factor,
        } => handle_create_topic(client, topic, replication_factor).await,
        Commands::Publish {
            topic,
            key,
            message,
        } => handle_publish(client, topic, key, message).await,
        Commands::Consume {
            topic,
            start,
            count,
            timeout_sec,
            group,
        } => handle_consume(client, topic, start, count, timeout_sec, group).await,
        Commands::Exit | Commands::Quit => {
            // Handled in main loop
            Ok(())
        }
    }
}

async fn handle_create_topic(
    client: &Arc<Client>,
    topic: String,
    replication_factor: u64,
) -> anyhow::Result<()> {
    let policy = StoragePolicy {
        retention_ms: None,
        replication_factor,
        partition_strategy: PartitionStrategy::AutoSplit,
    };

    println!("Creating topic '{}' (RF={})...", topic, replication_factor);
    client.create_topic(&topic, policy).await?;
    println!("Topic created successfully.");

    Ok(())
}

async fn handle_publish(
    client: &Arc<Client>,
    topic: String,
    key: String,
    message: String,
) -> anyhow::Result<()> {
    let producer = Producer::new(client.clone(), topic.clone(), ProducerConfig::default());
    let entry_id = producer.send(key.as_bytes(), message.into_bytes()).await?;

    println!(
        "Published to '{}' [key: '{}'] -> entry_id: {}",
        topic, key, entry_id
    );

    Ok(())
}

async fn handle_consume(
    client: &Arc<Client>,
    topic: String,
    start: String,
    count: usize,
    timeout_sec: Option<u64>,
    group: Option<String>,
) -> anyhow::Result<()> {
    let start_policy = start.parse()?;
    println!(
        "Consuming up to {} records from '{}' (start: {}, group: {:?})...",
        count, topic, start, group
    );

    let consumer = Consumer::new_with_group(
        client.clone(),
        topic,
        KeyInterest::AllKeys,
        start_policy,
        group.clone(),
    )
    .await?;

    let mut fetched = 0;
    let limit = if count == 0 { usize::MAX } else { count };

    while fetched < limit {
        let next_record_future = consumer.next_record();

        let result = tokio::select! {
            _ = shutdown_signal() => {
                println!("\nGracefully stopping consumer...");
                break;
            }
            r =  fetch_with_optional_timeout(next_record_future, timeout_sec) => r,
        };

        let Ok(result) = result else {
            // Timeout reached
            break;
        };

        match result {
            Ok(Some(record)) => {
                print_record(&record);
                fetched += 1;

                // Auto-commit periodically in unlimited mode
                if count == 0 && group.is_some() && fetched % 10 == 0 {
                    let _ = consumer.commit().await;
                }
            }
            Ok(None) => {
                println!("Topic drained.");
                break;
            }
            Err(e) => {
                println!("Consume error: {}", e);
                break;
            }
        }
    }

    if fetched == 0 {
        println!("(No records found)");
    }

    if group.is_some() && fetched > 0 {
        if let Err(e) = consumer.commit().await {
            println!("Failed to commit offsets: {}", e);
        } else {
            println!("Offsets committed successfully.");
        }
    }

    Ok(())
}

async fn fetch_with_optional_timeout<F, T>(
    fut: F,
    timeout_sec: Option<u64>,
) -> Result<T, tokio::time::error::Elapsed>
where
    F: Future<Output = T>,
{
    match timeout_sec {
        Some(sec) => tokio::time::timeout(std::time::Duration::from_secs(sec), fut).await,
        None => Ok(fut.await),
    }
}

fn print_record(record: &ConsumerRecord) {
    // Replace RecordType with your actual struct
    let sys_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();

    print!(
        "Fetched At:{}.{:03} ",
        sys_time.as_secs() % 86_400,
        sys_time.subsec_millis(),
    );
    println!(
        "[[{}] key: '{}', value: '{}'",
        record.offset,
        String::from_utf8_lossy(&record.key),
        String::from_utf8_lossy(&record.value)
    );
}

struct CliHelper;

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let commands = vec!["create-topic", "publish", "consume", "help", "exit", "quit"];
        let mut matches = Vec::new();

        // Only autocomplete the first word (command)
        if !line[..pos].contains(' ') {
            let prefix = &line[..pos];
            for cmd in commands {
                if cmd.starts_with(prefix) {
                    matches.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
        }
        Ok((0, matches))
    }
}

impl rustyline::hint::Hinter for CliHelper {
    type Hint = String;

    fn hint(&self, line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        let commands = vec!["create-topic", "publish", "consume", "help", "exit", "quit"];

        if line.is_empty() {
            return None;
        }

        if !line.contains(' ') {
            for cmd in commands {
                if cmd.starts_with(line) && cmd.len() > line.len() {
                    return Some(cmd[line.len()..].to_string());
                }
            }
            return None;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        let ends_with_space = line.ends_with(' ');
        let parts_count = parts.len();

        if parts_count == 0 {
            return None;
        }
        let cmd = parts[0];

        match cmd {
            "create-topic" => {
                if parts_count == 1 && ends_with_space {
                    return Some("<TOPIC> [REPLICATION_FACTOR]".to_string());
                } else if parts_count == 2 && ends_with_space {
                    return Some("[REPLICATION_FACTOR]".to_string());
                }
            }
            "publish" => {
                if parts_count == 1 && ends_with_space {
                    return Some("<TOPIC> <KEY> <MESSAGE>".to_string());
                } else if parts_count == 2 && ends_with_space {
                    return Some("<KEY> <MESSAGE>".to_string());
                } else if parts_count == 3 && ends_with_space {
                    return Some("<MESSAGE>".to_string());
                }
            }
            "consume" => {
                if parts_count == 1 && ends_with_space {
                    return Some("<TOPIC> [START] [COUNT] [-t TIMEOUT] [-g GROUP]".to_string());
                } else if parts_count == 2 && ends_with_space {
                    return Some("[START] [COUNT] [-t TIMEOUT] [-g GROUP]".to_string());
                } else if parts_count == 3 && ends_with_space {
                    return Some("[COUNT] [-t TIMEOUT] [-g GROUP]".to_string());
                } else if parts_count == 4 && ends_with_space {
                    return Some("[-t TIMEOUT] [-g GROUP]".to_string());
                }
            }
            _ => {}
        }
        None
    }
}

impl rustyline::highlight::Highlighter for CliHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        // \x1b[90m is the ANSI escape code for bright black (grey)
        std::borrow::Cow::Owned(format!("\x1b[90m{}\x1b[0m", hint))
    }
}
impl rustyline::validate::Validator for CliHelper {}
impl rustyline::Helper for CliHelper {}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
