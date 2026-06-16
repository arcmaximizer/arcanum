use crate::scheduler::{Proposal, RuntimeStatus, SchedulerHandle};
use crate::types::ProcessId;
use serde::Deserialize;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Deserialize)]
struct MgmtRequest {
    #[serde(rename = "type")]
    msg_type: String,
    target: String,
    #[serde(default)]
    data: serde_json::Value,
}

pub struct MgmtHandle {
    pub port: u16,
}

impl MgmtHandle {
    pub async fn new(scheduler: SchedulerHandle, port: u16) -> Self {
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .expect("failed to bind management port");
        let local_port = listener.local_addr().unwrap().port();
        let mgmt_process = ProcessId {
            namespace: "sys".into(),
            app: "mgmt".into(),
            proc: "entrypoint".into(),
        };

        tokio::spawn(run(listener, scheduler, mgmt_process));

        Self { port: local_port }
    }
}

async fn run(listener: TcpListener, scheduler: SchedulerHandle, mgmt_process: ProcessId) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tracing::debug!("mgmt: accepted connection from {addr}");
                let sched = scheduler.clone();
                let proc = mgmt_process.clone();
                tokio::spawn(handle_connection(stream, sched, proc));
            }
            Err(e) => {
                tracing::error!("mgmt: accept error: {e}");
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    scheduler: SchedulerHandle,
    mgmt_process: ProcessId,
) {
    let mut buf = Vec::new();
    loop {
        match read_frame(&mut stream, &mut buf).await {
            Some(data) => {
                let response = handle_message(&data, &scheduler, &mgmt_process).await;
                if write_frame(&mut stream, &response).await.is_err() {
                    break;
                }
            }
            None => break,
        }
    }
}

async fn read_frame(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return None;
    }
    buf.resize(len, 0);
    if stream.read_exact(buf).await.is_err() {
        return None;
    }
    Some(buf.clone())
}

async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    Ok(())
}

async fn handle_message(
    data: &[u8],
    scheduler: &SchedulerHandle,
    mgmt_process: &ProcessId,
) -> Vec<u8> {
    let req: MgmtRequest = match rmp_serde::from_slice(data) {
        Ok(v) => v,
        Err(e) => return encode_error(&format!("invalid request: {e}")),
    };

    let target = match ProcessId::try_from(req.target.as_str()) {
        Ok(p) => p,
        Err(_) => {
            // Try appending /entrypoint
            let with_entry = format!("{}/entrypoint", req.target);
            match ProcessId::try_from(with_entry.as_str()) {
                Ok(p) => p,
                Err(e) => return encode_error(&format!("invalid target: {e}")),
            }
        }
    };

    let input = rmp_serde::to_vec(&req.data).unwrap_or_default();

    match req.msg_type.as_str() {
        "call" => handle_call(scheduler, mgmt_process, target, input).await,
        "notify" => {
            let proposal = Proposal {
                process: target,
                event: None,
                input,
                promise: None,
                from: mgmt_process.clone(),
            };
            scheduler.add_proposal(proposal).await;
            encode_ack()
        }
        _ => encode_error(&format!("unknown message type: {}", req.msg_type)),
    }
}

async fn handle_call(
    scheduler: &SchedulerHandle,
    mgmt_process: &ProcessId,
    target: ProcessId,
    input: Vec<u8>,
) -> Vec<u8> {
    let proposal = Proposal {
        process: target.clone(),
        event: None,
        input,
        promise: None,
        from: mgmt_process.clone(),
    };

    let proposal = scheduler.add_proposal(proposal).await;
    let event = match &proposal.event {
        Some(e) => e.clone(),
        None => return encode_error("scheduler did not allocate event ID"),
    };

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            return encode_error("timeout");
        }

        if let Some(chunks) = scheduler.get_chunks(event.clone()).await
            && let Some(last) = chunks.last()
        {
            match last.status {
                RuntimeStatus::End => {
                    return encode_ok(&last.returns);
                }
                RuntimeStatus::Error => {
                    let msg = String::from_utf8_lossy(&last.returns).to_string();
                    return encode_error(&msg);
                }
                RuntimeStatus::Normal => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn encode_ok(returns: &[u8]) -> Vec<u8> {
    let inner_data = if returns.is_empty() {
        serde_json::Value::Null
    } else if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(returns) {
        v.get("data").cloned().unwrap_or(serde_json::Value::Null)
    } else {
        serde_json::Value::String(String::from_utf8_lossy(returns).into())
    };
    let resp = serde_json::json!({"ok": true, "data": inner_data});
    rmp_serde::to_vec(&resp).unwrap_or_default()
}

fn encode_ack() -> Vec<u8> {
    let resp = serde_json::json!({"ok": true});
    rmp_serde::to_vec(&resp).unwrap_or_default()
}

fn encode_error(msg: &str) -> Vec<u8> {
    let resp = serde_json::json!({"ok": false, "error": msg});
    rmp_serde::to_vec(&resp).unwrap_or_default()
}
