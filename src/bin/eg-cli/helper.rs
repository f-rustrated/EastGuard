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
        complete_impl(line, pos)
    }
}

pub fn complete_impl(line: &str, pos: usize) -> rustyline::Result<(usize, Vec<Pair>)> {
    let commands = vec!["create-topic", "publish", "consume", "help", "exit", "quit"];
    let mut matches = Vec::new();

    let line_prefix = &line[..pos];
    if !line_prefix.contains(' ') {
        let prefix = line_prefix;
        for cmd in commands {
            if cmd.starts_with(prefix) {
                matches.push(Pair {
                    display: cmd.to_string(),
                    replacement: cmd.to_string(),
                });
            }
        }
        Ok((0, matches))
    } else {
        if let Some(last_space_idx) = line_prefix.rfind(' ') {
            let token_start = last_space_idx + 1;
            let token = &line_prefix[token_start..];

            let parts: Vec<&str> = line_prefix.split_whitespace().collect();
            if !parts.is_empty() {
                let cmd = parts[0];
                if cmd == "consume" && token.starts_with('-') {
                    let options = vec![
                        "--pause-range",
                        "--resume-after-ms",
                        "--seek-range",
                        "--seek-offset",
                        "--auto-commit-ms",
                        "--delivery",
                        "-t",
                        "-g",
                    ];
                    for opt in options {
                        if opt.starts_with(token) {
                            matches.push(Pair {
                                display: opt.to_string(),
                                replacement: opt.to_string(),
                            });
                        }
                    }
                }
            }
            Ok((token_start, matches))
        } else {
            Ok((0, matches))
        }
    }
}

impl rustyline::hint::Hinter for CliHelper {
    type Hint = String;

    fn hint(&self, line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        hint_impl(line)
    }
}

pub fn hint_impl(line: &str) -> Option<String> {
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
            if !ends_with_space {
                let last_part = parts[parts_count - 1];
                if last_part.starts_with('-') {
                    let options = vec![
                        "--pause-range",
                        "--resume-after-ms",
                        "--seek-range",
                        "--seek-offset",
                        "--auto-commit-ms",
                        "--delivery",
                        "-t",
                        "-g",
                    ];
                    for opt in options {
                        if opt.starts_with(last_part) && opt.len() > last_part.len() {
                            return Some(opt[last_part.len()..].to_string());
                        }
                    }
                }
                return None;
            }

            let last_part = parts[parts_count - 1];
            match last_part {
                "-t" => return Some("TIMEOUT".to_string()),
                "-g" => return Some("GROUP".to_string()),
                "--pause-range" => return Some("RANGE_ID".to_string()),
                "--resume-after-ms" => return Some("MS".to_string()),
                "--seek-range" => return Some("RANGE_ID".to_string()),
                "--seek-offset" => return Some("OFFSET".to_string()),
                "--auto-commit-ms" => return Some("MS".to_string()),
                "--delivery" => return Some("at-least-once|at-most-once".to_string()),
                _ => {}
            }

            if parts_count == 1 {
                return Some("<TOPIC> [START] [COUNT] [-t TIMEOUT] [-g GROUP] [--pause-range RANGE_ID] [--resume-after-ms MS] [--seek-range RANGE_ID] [--seek-offset OFFSET]".to_string());
            } else if parts_count == 2 {
                return Some("[START] [COUNT] [-t TIMEOUT] [-g GROUP] [--pause-range RANGE_ID] [--resume-after-ms MS] [--seek-range RANGE_ID] [--seek-offset OFFSET]".to_string());
            } else if parts_count == 3 {
                return Some("[COUNT] [-t TIMEOUT] [-g GROUP] [--pause-range RANGE_ID] [--resume-after-ms MS] [--seek-range RANGE_ID] [--seek-offset OFFSET]".to_string());
            } else {
                return Some("[-t TIMEOUT] [-g GROUP] [--pause-range RANGE_ID] [--resume-after-ms MS] [--seek-range RANGE_ID] [--seek-offset OFFSET]".to_string());
            }
        }
        _ => {}
    }
    None
}

impl rustyline::highlight::Highlighter for CliHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> std::borrow::Cow<'h, str> {
        // \x1b[90m is the ANSI escape code for bright black (grey)
        std::borrow::Cow::Owned(format!("\x1b[90m{}\x1b[0m", hint))
    }
}

impl rustyline::validate::Validator for CliHelper {}
impl rustyline::Helper for CliHelper {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_complete_command() {
        let (pos, matches) = complete_impl("con", 3).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].replacement, "consume");
    }

    #[test]
    fn test_complete_option() {
        let (pos1, matches1) = complete_impl("consume --p", 11).unwrap();
        assert_eq!(pos1, 8);
        assert_eq!(matches1.len(), 1);
        assert_eq!(matches1[0].replacement, "--pause-range");

        let (pos2, matches2) = complete_impl("consume -", 9).unwrap();
        assert_eq!(pos2, 8);
        assert!(matches2.iter().any(|m| m.replacement == "--pause-range"));
        assert!(
            matches2
                .iter()
                .any(|m| m.replacement == "--resume-after-ms")
        );
        assert!(matches2.iter().any(|m| m.replacement == "--seek-range"));
        assert!(matches2.iter().any(|m| m.replacement == "--seek-offset"));
    }

    #[test]
    fn test_hint_command() {
        let hint = hint_impl("con");
        assert_eq!(hint, Some("sume".to_string()));
    }

    #[test]
    fn test_hint_positional() {
        let hint = hint_impl("consume ");
        assert!(hint.unwrap().contains("<TOPIC> [START]"));

        let hint2 = hint_impl("consume my_topic ");
        let hint2_str = hint2.unwrap();
        assert!(hint2_str.contains("[START] [COUNT]"));
        assert!(!hint2_str.contains("<TOPIC>"));
    }

    #[test]
    fn test_hint_flag_value() {
        let hint_t = hint_impl("consume my_topic earliest 0 -t ");
        assert_eq!(hint_t, Some("TIMEOUT".to_string()));

        let hint_pause = hint_impl("consume my_topic earliest 0 --pause-range ");
        assert_eq!(hint_pause, Some("RANGE_ID".to_string()));
    }

    #[test]
    fn test_hint_flag_completion() {
        let hint = hint_impl("consume my_topic earliest 0 --pau");
        assert_eq!(hint, Some("se-range".to_string()));
    }
}
