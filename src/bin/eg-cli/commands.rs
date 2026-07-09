use clap::{Parser, Subcommand};
use east_guard::client::{
    Client, Consumer, ConsumerConfig, ConsumerRecord, KeyInterest, PartitionStrategy, Producer,
    ProducerConfig, RangeId, StoragePolicy,
};
use std::sync::Arc;
use std::future::Future;

#[derive(Parser, Debug)]
#[command(name = "", disable_help_flag = true, disable_version_flag = true)]
pub struct CliCommand {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
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
        /// Optional auto-commit interval in milliseconds (default: 1000)
        #[arg(long, default_value = "1000")]
        auto_commit_ms: u64,
        /// Pause this range before consuming; mainly useful for manual testing.
        #[arg(long)]
        pause_range: Option<u64>,
        /// Resume the paused range after this many milliseconds.
        #[arg(long)]
        resume_after_ms: Option<u64>,
        /// Seek this range to an absolute record offset before consuming; mainly useful for replay/debugging.
        #[arg(long)]
        seek_range: Option<u64>,
        /// Absolute record offset to seek to when --seek-range is set.
        #[arg(long)]
        seek_offset: Option<u64>,
    },
    /// Exit the CLI
    Exit,
    Quit,
}

pub async fn execute_command(cmd: Commands, client: &Arc<Client>) -> anyhow::Result<()> {
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
            auto_commit_ms,
            pause_range,
            resume_after_ms,
            seek_range,
            seek_offset,
        } => {
            let control =
                ConsumeControl::from_flags(pause_range, resume_after_ms, seek_range, seek_offset)?;
            let request = ConsumeRequest {
                topic,
                start,
                count,
                timeout_sec,
                group,
                auto_commit_ms,
                control,
            };
            handle_consume(client, request).await
        }
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

struct ConsumeRequest {
    topic: String,
    start: String,
    count: usize,
    timeout_sec: Option<u64>,
    group: Option<String>,
    auto_commit_ms: u64,
    control: ConsumeControl,
}

enum ConsumeControl {
    None,
    Pause {
        range: u64,
        resume_after_ms: Option<u64>,
    },
    Seek {
        range: u64,
        absolute_offset: u64,
    },
}

impl ConsumeControl {
    fn from_flags(
        pause_range: Option<u64>,
        resume_after_ms: Option<u64>,
        seek_range: Option<u64>,
        seek_offset: Option<u64>,
    ) -> anyhow::Result<Self> {
        match (pause_range, resume_after_ms, seek_range, seek_offset) {
            (None, None, None, None) => Ok(Self::None),
            (Some(range), resume_after_ms, None, None) => Ok(Self::Pause {
                range,
                resume_after_ms,
            }),
            (None, None, Some(range), offset) => Ok(Self::Seek {
                range,
                absolute_offset: offset.unwrap_or(0),
            }),
            (None, Some(_), None, None) => {
                anyhow::bail!("--resume-after-ms requires --pause-range")
            }
            (None, _, None, Some(_)) => anyhow::bail!("--seek-offset requires --seek-range"),
            _ => anyhow::bail!("pause/resume and seek controls are mutually exclusive"),
        }
    }
}

async fn handle_consume(client: &Arc<Client>, request: ConsumeRequest) -> anyhow::Result<()> {
    let ConsumeRequest {
        topic,
        start,
        count,
        timeout_sec,
        group,
        auto_commit_ms,
        control,
    } = request;

    let start_policy = start.parse()?;
    println!(
        "Consuming up to {} records from '{}' (start: {}, group: {:?})...",
        count, topic, start, group
    );

    let config = ConsumerConfig {
        start_policy,
        group_id: group.clone(),
        auto_commit_interval_ms: auto_commit_ms,
    };

    let consumer =
        Consumer::new(client.clone(), topic.clone(), KeyInterest::AllKeys, config).await?;

    match control {
        ConsumeControl::None => {}
        ConsumeControl::Pause {
            range,
            resume_after_ms,
        } => {
            consumer.pause_range(RangeId::from(range)).await?;
            println!("Paused range {}.", range);

            if let Some(delay_ms) = resume_after_ms {
                let consumer = consumer.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    if let Err(error) = consumer.resume_range(RangeId::from(range)).await {
                        println!("Failed to resume range {}: {}", range, error);
                    } else {
                        println!("Resumed range {}.", range);
                    }
                });
            }
        }
        ConsumeControl::Seek {
            range,
            absolute_offset,
        } => {
            consumer
                .seek_range(RangeId::from(range), absolute_offset)
                .await?;
            println!(
                "Seeked range {} to absolute offset {}.",
                range, absolute_offset
            );
        }
    }

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
    let sys_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();

    print!(
        "Fetched At:{}.{:03} ",
        sys_time.as_secs() % 86_400,
        sys_time.subsec_millis(),
    );
    println!(
        "[[{}:{} abs={}] key: '{}', value: '{}'",
        record.position.entry_id.0,
        record.position.batch_offset,
        record.position.absolute_offset,
        String::from_utf8_lossy(&record.key),
        String::from_utf8_lossy(&record.value)
    );
}

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
