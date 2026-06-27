use std::path::PathBuf;

use rustyline::{DefaultEditor, error::ReadlineError};
use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const DEFAULT_TIMEOUT_SECS: u64 = 30;

pub async fn run(host: String, port: u16, default_timeout_secs: u64, one_shot: Vec<String>) {
    if !one_shot.is_empty() {
        run_one_shot(&host, port, default_timeout_secs, &one_shot).await;
    } else {
        run_repl(host, port, default_timeout_secs).await;
    }
}

async fn connect(host: &str, port: u16) -> Option<TcpStream> {
    match TcpStream::connect(format!("{host}:{port}")).await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("error: could not connect to {host}:{port}: {e}");
            None
        }
    }
}

async fn run_one_shot(host: &str, port: u16, timeout_secs: u64, args: &[String]) {
    let (msg_type, target, data_str) = match parse_args(args) {
        Some(v) => v,
        None => {
            eprintln!("usage: arcanum shell call <target> [data]");
            eprintln!("       arcanum shell notify <target> [data]");
            return;
        }
    };

    let data = parse_data(&data_str);

    let mut stream = match connect(host, port).await {
        Some(s) => s,
        None => return,
    };

    execute(&mut stream, &msg_type, &target, &data, timeout_secs).await;
}

fn parse_args(args: &[String]) -> Option<(String, String, String)> {
    if args.is_empty() {
        return None;
    }
    let msg_type = args[0].to_lowercase();
    if msg_type != "call" && msg_type != "notify" {
        return None;
    }
    if args.len() < 2 {
        return None;
    }
    let target = args[1].clone();
    let data_str: String = if args.len() > 2 {
        args[2..].join(" ")
    } else {
        String::new()
    };
    Some((msg_type, target, data_str))
}

fn parse_data(input: &str) -> JsonValue {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return JsonValue::Null;
    }
    if let Ok(v) = serde_json::from_str::<JsonValue>(trimmed) {
        return v;
    }
    JsonValue::String(trimmed.to_string())
}

fn history_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".arcanum_shell_history")
}

async fn run_repl(host: String, port: u16, default_timeout_secs: u64) {
    let mut rl = match DefaultEditor::new() {
        Ok(ed) => ed,
        Err(e) => {
            eprintln!("error initializing line editor: {e}");
            return;
        }
    };

    let history_file = history_path();
    let _ = rl.load_history(history_file.as_path());

    println!("Arcanum shell — {host}:{port}");
    println!("Type 'help' for commands, Ctrl-D or 'exit' to quit.");
    println!();

    let mut stream: Option<TcpStream> = None;

    loop {
        let line = match rl.readline("> ") {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let _ = rl.add_history_entry(trimmed);

        if handle_builtin(trimmed) {
            continue;
        }

        let (timeout_secs, tokens) = extract_timeout(shell_tokenize(trimmed), default_timeout_secs);
        if tokens.is_empty() {
            continue;
        }

        let msg_type = tokens[0].to_lowercase();
        if msg_type != "call" && msg_type != "notify" {
            println!("unknown command: {msg_type}");
            println!("available: call, notify, help, exit, quit");
            continue;
        }

        if tokens.len() < 2 {
            println!("usage: {msg_type} [--timeout <s>] <target> [data]");
            continue;
        }

        let target = &tokens[1];
        let data_str = if tokens.len() > 2 {
            tokens[2..].join(" ")
        } else {
            String::new()
        };
        let data = parse_data(&data_str);

        if stream.is_none() {
            stream = connect(&host, port).await;
        }

        let s = match &mut stream {
            Some(s) => s,
            None => continue,
        };

        execute(s, &msg_type, target, &data, timeout_secs).await;
    }

    let _ = rl.save_history(history_file.as_path());
}

fn extract_timeout(tokens: Vec<String>, default_secs: u64) -> (u64, Vec<String>) {
    let mut timeout = default_secs;
    let mut remaining = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        if (tokens[i] == "--timeout" || tokens[i] == "-t") && i + 1 < tokens.len() {
            if let Ok(s) = tokens[i + 1].parse::<u64>() {
                timeout = s;
                i += 2;
                continue;
            }
        } else if let Some(s) = tokens[i].strip_prefix("-t") {
            if let Ok(ms) = s.parse::<u64>() {
                timeout = ms;
                i += 1;
                continue;
            }
        } else if let Some(s) = tokens[i].strip_prefix("--timeout=")
            && let Ok(ms) = s.parse::<u64>()
        {
            timeout = ms;
            i += 1;
            continue;
        }
        remaining.push(tokens[i].clone());
        i += 1;
    }
    (timeout, remaining)
}

async fn execute(
    stream: &mut TcpStream,
    msg_type: &str,
    target: &str,
    data: &JsonValue,
    timeout_secs: u64,
) {
    let req = serde_json::json!({
        "type": msg_type,
        "target": target,
        "data": data,
        "timeoutMs": timeout_secs * 1000,
    });
    let req_bytes = rmp_serde::to_vec(&req).unwrap();

    if send_frame(stream, &req_bytes).await.is_err() {
        eprintln!("error: connection lost");
        return;
    }

    match read_frame(stream).await {
        Some(resp_bytes) => match rmp_serde::from_slice::<JsonValue>(&resp_bytes) {
            Ok(v) => print_response(&v),
            Err(_) => {
                println!("{}", String::from_utf8_lossy(&resp_bytes));
            }
        },
        None => {
            eprintln!("error: no response from server");
        }
    }
}

fn print_response(v: &JsonValue) {
    if let Some(obj) = v.as_object() {
        if obj.get("ok") == Some(&JsonValue::Bool(true)) {
            if let Some(data) = obj.get("data") {
                match data {
                    JsonValue::Null => println!("null"),
                    JsonValue::String(s) => println!("{s}"),
                    _ => {
                        let formatted = serde_json::to_string_pretty(data)
                            .unwrap_or_else(|_| format!("{data}"));
                        println!("{formatted}");
                    }
                }
            } else {
                println!("ok");
            }
        } else if let Some(err) = obj.get("error") {
            println!("error: {}", err.as_str().unwrap_or("unknown"));
        } else {
            let formatted = serde_json::to_string_pretty(v).unwrap_or_else(|_| format!("{v}"));
            println!("{formatted}");
        }
    } else {
        let formatted = serde_json::to_string_pretty(v).unwrap_or_else(|_| format!("{v}"));
        println!("{formatted}");
    }
}

fn handle_builtin(line: &str) -> bool {
    let lower = line.to_lowercase();
    if lower == "exit" || lower == "quit" {
        std::process::exit(0);
    }
    if lower == "help" {
        println!("commands:");
        println!("  call [--timeout <s>] <target> [data]     send a message and wait for response");
        println!("  notify <target> [data]                   send a message without waiting");
        println!("  help                                     show this help");
        println!("  exit, quit                               exit the shell");
        println!();
        println!("options:");
        println!("  --timeout, -t <s>  call timeout in seconds (default: {DEFAULT_TIMEOUT_SECS})");
        println!();
        println!("target: ^namespace/app[/process]   (omit /process to target entrypoint)");
        println!("data:   json value or plain string (default: null)");
        return true;
    }
    false
}

fn shell_tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }

        if chars[i] == '\'' || chars[i] == '"' {
            let quote = chars[i];
            i += 1;
            let mut token = String::new();
            while i < chars.len() && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                    token.push(chars[i]);
                } else {
                    token.push(chars[i]);
                }
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            tokens.push(token);
        } else {
            let mut token = String::new();
            while i < chars.len() && !chars[i].is_whitespace() {
                token.push(chars[i]);
                i += 1;
            }
            tokens.push(token);
        }
    }

    tokens
}

async fn send_frame(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    Ok(())
}

async fn read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return None;
    }
    let mut buf = vec![0u8; len];
    if stream.read_exact(&mut buf).await.is_err() {
        return None;
    }
    Some(buf)
}
