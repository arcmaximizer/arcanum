use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
};
use reqwest::StatusCode;
use tokio::sync::{RwLock, mpsc};

use crate::{
    manager::StatelessCall,
    scheduler::{Proposal, RuntimeStatus, SchedulerHandle},
    types::ProcessId,
};

fn body_to_msgpack(body: &[u8]) -> Vec<u8> {
    let body_str = String::from_utf8_lossy(body);
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body_str) {
        rmp_serde::to_vec(&json).unwrap_or_else(|_| body.to_vec())
    } else {
        rmp_serde::to_vec(&serde_json::Value::String(body_str.into()))
            .unwrap_or_else(|_| body.to_vec())
    }
}

fn encode_response(value: &serde_json::Value) -> Vec<u8> {
    let wrapped = serde_json::json!({ "data": value });
    rmp_serde::to_vec(&wrapped).unwrap_or_default()
}

#[derive(Clone)]
struct AppState {
    scheduler: SchedulerHandle,
    routes: Arc<RwLock<Vec<(String, ProcessId)>>>,
}

pub struct HttpServerHandle {
    sender: mpsc::UnboundedSender<StatelessCall>,
    pub port: u16,
}

impl HttpServerHandle {
    pub async fn new(scheduler: SchedulerHandle, port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .expect("failed to bind HTTP server port");
        let port = listener.local_addr().unwrap().port();
        Self::from_listener(scheduler, listener)
    }

    pub fn from_listener(scheduler: SchedulerHandle, listener: tokio::net::TcpListener) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(run(listener, rx, scheduler));
        Self { sender: tx, port }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<StatelessCall> {
        self.sender.clone()
    }
}

async fn run(
    listener: tokio::net::TcpListener,
    mut rx: mpsc::UnboundedReceiver<StatelessCall>,
    scheduler: SchedulerHandle,
) {
    let routes: Arc<RwLock<Vec<(String, ProcessId)>>> = Arc::new(RwLock::new(Vec::new()));

    let state = AppState {
        scheduler: scheduler.clone(),
        routes: routes.clone(),
    };

    let app = Router::new()
        .route("/{*path}", post(handle_request))
        .with_state(state);

    tokio::spawn(handle_process_messages(
        rx,
        scheduler.clone(),
        routes.clone(),
    ));

    axum::serve(listener, app).await.unwrap();
}

async fn handle_process_messages(
    mut rx: mpsc::UnboundedReceiver<StatelessCall>,
    scheduler: SchedulerHandle,
    routes: Arc<RwLock<Vec<(String, ProcessId)>>>,
) {
    while let Some(call) = rx.recv().await {
        let input: serde_json::Value = match rmp_serde::from_slice(&call.proposal.input) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let action = input["action"].as_str().unwrap_or("");

        let result = match action {
            "add" => {
                let app_str = input["app"].as_str().unwrap_or("");
                let host = input["host"].as_str().unwrap_or("");
                match ProcessId::try_from(app_str) {
                    Ok(pid) => {
                        routes.write().await.push((host.to_string(), pid));
                        serde_json::json!({"ok": true})
                    }
                    Err(_) => {
                        serde_json::json!({"error": format!("invalid process: {}", app_str)})
                    }
                }
            }
            "list-uris" => {
                let app_str = input["app"].as_str().unwrap_or("");
                let uris: Vec<String> = if let Ok(pid) = ProcessId::try_from(app_str) {
                    routes
                        .read()
                        .await
                        .iter()
                        .filter(|(_, p)| p == &pid)
                        .map(|(host, _)| host.clone())
                        .collect()
                } else {
                    Vec::new()
                };
                serde_json::json!({"uris": uris})
            }
            "remove" => {
                let app_str = input["app"].as_str().unwrap_or("");
                let host = input["host"].as_str().unwrap_or("");
                let before = routes.read().await.len();
                if let Ok(pid) = ProcessId::try_from(app_str) {
                    routes
                        .write()
                        .await
                        .retain(|(h, p)| !(h == host && p == &pid));
                }
                let removed = before - routes.read().await.len();
                serde_json::json!({"ok": true, "removed": removed})
            }
            _ => serde_json::json!({"error": format!("unknown action: {}", action)}),
        };

        let _ = scheduler
            .stateless_satisfy(call.proposal, encode_response(&result))
            .await;
    }
}

async fn handle_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let process = {
        let routes = state.routes.read().await;
        routes
            .iter()
            .find(|(h, _)| h == &host)
            .map(|(_, p)| p.clone())
    };

    let process = match process {
        Some(p) => p,
        None => return (StatusCode::NOT_FOUND, "no route for host").into_response(),
    };

    let event = state.scheduler.get_next_event_id(process.clone()).await;
    let input = body_to_msgpack(&body);

    let proposal = Proposal {
        process: process.clone(),
        event: Some(event.clone()),
        input,
        promise: None,
    };

    state.scheduler.add_proposal(proposal).await;

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            return (StatusCode::GATEWAY_TIMEOUT, "timeout").into_response();
        }

        if let Some(chunks) = state.scheduler.get_chunks(event.clone()).await {
            if let Some(last) = chunks.last() {
                match last.status {
                    RuntimeStatus::End => {
                        let returns = &last.returns;
                        if returns.is_empty() {
                            return (StatusCode::NO_CONTENT, "").into_response();
                        }
                        if let Ok(json) = rmp_serde::from_slice::<serde_json::Value>(returns) {
                            return (StatusCode::OK, axum::Json(json)).into_response();
                        }
                        return (StatusCode::OK, returns.clone()).into_response();
                    }
                    RuntimeStatus::Error => {
                        let error_msg = String::from_utf8_lossy(&last.returns).to_string();
                        return (StatusCode::INTERNAL_SERVER_ERROR, error_msg).into_response();
                    }
                    RuntimeStatus::Normal => {}
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
