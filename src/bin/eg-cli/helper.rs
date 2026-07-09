use rustyline::Context;
use rustyline::completion::{Completer, Pair};

pub struct CliHelper;

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

    // TODO add pause/resume/seek to hint and auto completion
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
