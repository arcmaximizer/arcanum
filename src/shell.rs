use std::io::{BufRead, Write};

use serde_json::Value as JsonValue;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn run(host: String, port: u16, one_shot: Vec<String>) {
    if !one_shot.is_empty() {
        run_one_shot(&host, port, &one_shot).await;
    } else {
        run_repl(host, port).await;
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

async fn run_one_shot(host: &str, port: u16, args: &[String]) {
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

    let req = serde_json::json!({
        "type": msg_type,
        "target": target,
        "data": data,
    });
    let req_bytes = rmp_serde::to_vec(&req).unwrap();

    if send_frame(&mut stream, &req_bytes).await.is_err() {
        eprintln!("error: failed to send request");
        return;
    }

    match read_frame(&mut stream).await {
        Some(resp_bytes) => match rmp_serde::from_slice::<JsonValue>(&resp_bytes) {
            Ok(v) => {
                let formatted = serde_json::to_string_pretty(&v).unwrap_or_else(|_| format!("{v}"));
                println!("{formatted}");
            }
            Err(_) => {
                println!("{}", String::from_utf8_lossy(&resp_bytes));
            }
        },
        None => {
            eprintln!("error: no response from server");
        }
    }
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

async fn run_repl(host: String, port: u16) {
    println!("Arcanum shell — {host}:{port}");
    println!("Type 'help' for commands, 'exit' to quit.");

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut stream: Option<TcpStream> = None;

    loop {
        print!("> ");
        stdout.flush().ok();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(_) => break,
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if handle_builtin(trimmed) {
            continue;
        }

        let tokens = shell_tokenize(trimmed);
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
            println!("usage: {msg_type} <target> [data]");
            continue;
        }

        let target = &tokens[1];
        let data_str = if tokens.len() > 2 {
            tokens[2..].join(" ")
        } else {
            String::new()
        };
        let data = parse_data(&data_str);

        // Connect lazily or reconnect if needed
        if stream.is_none() {
            stream = connect(&host, port).await;
        }

        let s = match &mut stream {
            Some(s) => s,
            None => continue,
        };

        let req = serde_json::json!({
            "type": msg_type,
            "target": target,
            "data": data,
        });
        let req_bytes = rmp_serde::to_vec(&req).unwrap();

        if send_frame(s, &req_bytes).await.is_err() {
            eprintln!("error: connection lost");
            stream = None;
            continue;
        }

        match read_frame(s).await {
            Some(resp_bytes) => match rmp_serde::from_slice::<JsonValue>(&resp_bytes) {
                Ok(v) => {
                    if let Some(obj) = v.as_object() {
                        if obj.get("ok") == Some(&JsonValue::Bool(true)) {
                            if let Some(data) = obj.get("data") {
                                let formatted = serde_json::to_string_pretty(data)
                                    .unwrap_or_else(|_| format!("{data}"));
                                println!("{formatted}");
                            } else {
                                println!("ok");
                            }
                        } else if let Some(err) = obj.get("error") {
                            println!("error: {}", err.as_str().unwrap_or("unknown"));
                        }
                    } else {
                        let formatted =
                            serde_json::to_string_pretty(&v).unwrap_or_else(|_| format!("{v}"));
                        println!("{formatted}");
                    }
                }
                Err(_) => {
                    println!("{}", String::from_utf8_lossy(&resp_bytes));
                }
            },
            None => {
                eprintln!("error: connection lost");
                stream = None;
            }
        }
    }

    println!();
}

fn handle_builtin(line: &str) -> bool {
    let lower = line.to_lowercase();
    if lower == "exit" || lower == "quit" {
        std::process::exit(0);
    }
    if lower == "help" {
        println!("commands:");
        println!("  call <target> [data]     send a message and wait for response");
        println!("  notify <target> [data]   send a message without waiting");
        println!("  help                     show this help");
        println!("  exit, quit               exit the shell");
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
                i += 1; // skip closing quote
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
