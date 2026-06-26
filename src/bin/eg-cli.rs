use clap::{Parser, Subcommand};
use east_guard::client::{
    Client, Consumer, KeyInterest, PartitionStrategy, Producer, ProducerConfig, StartPolicy,
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
        #[arg(default_value = "10")]
        count: usize,
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
        } => {
            let policy = StoragePolicy {
                retention_ms: None,
                replication_factor,
                partition_strategy: PartitionStrategy::AutoSplit,
            };
            println!("Creating topic '{}' (RF={})...", topic, replication_factor);
            client.create_topic(&topic, policy).await?;
            println!("Topic created successfully.");
        }
        Commands::Publish {
            topic,
            key,
            message,
        } => {
            let producer = Producer::new(client.clone(), topic.clone(), ProducerConfig::default());
            let entry_id = producer.send(key.as_bytes(), message.into_bytes()).await?;
            println!(
                "Published to '{}' [key: '{}'] -> entry_id: {}",
                topic, key, entry_id
            );
        }
        Commands::Consume {
            topic,
            start,
            count,
        } => {
            let start_policy = match start.to_lowercase().as_str() {
                "latest" => StartPolicy::Latest,
                "earliest" => StartPolicy::Earliest,
                _ => anyhow::bail!("Start policy must be 'earliest' or 'latest'"),
            };

            println!(
                "Consuming up to {} records from '{}' (start: {})...",
                count, topic, start
            );
            let consumer =
                Consumer::new(client.clone(), topic, KeyInterest::AllKeys, start_policy).await?;

            let mut fetched = 0;
            while fetched < count {
                // Use a timeout so the REPL doesn't hang forever if the topic is empty
                match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    consumer.next_record(),
                )
                .await
                {
                    Ok(Ok(Some(record))) => {
                        println!(
                            "[{}] key: '{}', value: '{}'",
                            record.offset,
                            String::from_utf8_lossy(&record.key),
                            String::from_utf8_lossy(&record.value)
                        );
                        fetched += 1;
                    }
                    Ok(Ok(None)) => {
                        println!("Topic drained.");
                        break;
                    }
                    Ok(Err(e)) => {
                        println!("Consume error: {}", e);
                        break;
                    }
                    Err(_) => {
                        // Timeout reached, no more records immediately available
                        break;
                    }
                }
            }
            if fetched == 0 {
                println!("(No records found)");
            } else if fetched == count {
                println!("(Reached limit of {} records)", count);
            } else {
                println!("(Fetched {} records)", fetched);
            }
        }
        Commands::Exit | Commands::Quit => {
            // Handled in main loop
        }
    }
    Ok(())
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
}
impl rustyline::highlight::Highlighter for CliHelper {}
impl rustyline::validate::Validator for CliHelper {}
impl rustyline::Helper for CliHelper {}
