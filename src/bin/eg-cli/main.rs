use clap::Parser;
use east_guard::client::Client;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::net::SocketAddr;
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
