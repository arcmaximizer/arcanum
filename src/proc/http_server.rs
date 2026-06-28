use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::any,
};
use tokio::sync::{RwLock, mpsc};

use crate::{
    manager::StatelessCall,
    scheduler::{Proposal, RuntimeStatus, SchedulerHandle},
    types::ProcessId,
};

fn encode_response(value: &serde_json::Value) -> Vec<u8> {
    let wrapped = serde_json::json!({"data": value});
    rmp_serde::to_vec(&wrapped).unwrap_or_default()
}

fn build_request_input(method: &Method, uri: &Uri, headers: &HeaderMap, body: &Bytes) -> Vec<u8> {
    let query: serde_json::Map<String, serde_json::Value> = uri
        .query()
        .map(|q| {
            url::form_urlencoded::parse(q.as_bytes())
                .into_owned()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect()
        })
        .unwrap_or_default();

    let header_map: serde_json::Map<String, serde_json::Value> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str().ok().map(|val| {
                (
                    k.as_str().to_string(),
                    serde_json::Value::String(val.to_string()),
                )
            })
        })
        .collect();

    let body_str = String::from_utf8_lossy(body).into_owned();

    rmp_serde::to_vec(&serde_json::json!({
        "method": method.as_str(),
        "path": uri.path(),
        "query": query,
        "headers": header_map,
        "body": body_str,
    }))
    .unwrap_or_default()
}

fn build_response(returns: &[u8]) -> Response {
    if returns.is_empty() {
        return (StatusCode::NO_CONTENT, "").into_response();
    }

    let json = match rmp_serde::from_slice::<serde_json::Value>(returns) {
        Ok(v) => v,
        Err(_) => return (StatusCode::OK, returns.to_vec()).into_response(),
    };

    let data = json.get("data").cloned().unwrap_or(serde_json::Value::Null);

    match data {
        // Table with "body" key → full control response
        serde_json::Value::Object(ref obj) if obj.contains_key("body") => {
            let status = obj
                .get("status")
                .and_then(|s| s.as_u64())
                .and_then(|s| StatusCode::from_u16(s as u16).ok())
                .unwrap_or(StatusCode::OK);

            let body_val = obj.get("body").cloned().unwrap_or(serde_json::Value::Null);

            let mut response = axum::Json(serde_json::json!({"data": body_val})).into_response();
            *response.status_mut() = status;

            if let Some(headers_obj) = obj.get("headers").and_then(|h| h.as_object()) {
                for (k, v) in headers_obj {
                    if let Some(val) = v.as_str()
                        && let (Ok(name), Ok(value)) = (
                            HeaderName::from_bytes(k.as_bytes()),
                            HeaderValue::from_str(val),
                        )
                    {
                        response.headers_mut().insert(name, value);
                    }
                }
            }

            response
        }
        // Primitive or table without "body" → old behaviour (200 OK)
        _ => (StatusCode::OK, axum::Json(json)).into_response(),
    }
}

#[derive(Clone)]
struct AppState {
    scheduler: SchedulerHandle,
    routes: Arc<RwLock<Vec<(String, ProcessId)>>>,
    process: ProcessId,
}

pub struct HttpServerHandle {
    sender: mpsc::UnboundedSender<StatelessCall>,
    pub port: u16,
}

impl HttpServerHandle {
    pub async fn new(scheduler: SchedulerHandle, port: u16, process: ProcessId) -> Self {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .expect("failed to bind HTTP server port");
        Self::from_listener(scheduler, listener, process)
    }

    pub fn from_listener(
        scheduler: SchedulerHandle,
        listener: tokio::net::TcpListener,
        process: ProcessId,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(run(listener, rx, scheduler, process));
        Self { sender: tx, port }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<StatelessCall> {
        self.sender.clone()
    }
}

async fn run(
    listener: tokio::net::TcpListener,
    rx: mpsc::UnboundedReceiver<StatelessCall>,
    scheduler: SchedulerHandle,
    process: ProcessId,
) {
    let routes: Arc<RwLock<Vec<(String, ProcessId)>>> = Arc::new(RwLock::new(Vec::new()));

    let state = AppState {
        scheduler: scheduler.clone(),
        routes: routes.clone(),
        process,
    };

    let app = Router::new()
        .route("/", any(handle_request))
        .route("/{*path}", any(handle_request))
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
                        tracing::debug!(
                            "http_server: registered route {} for process {}",
                            host,
                            pid
                        );
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
    method: Method,
    uri: Uri,
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

    tracing::debug!(
        "http_server: handling request {} {} for host {}",
        method,
        uri.path(),
        host,
    );

    let process = match process {
        Some(p) => p,
        None => return (StatusCode::NOT_FOUND, "no route for host").into_response(),
    };

    let input = build_request_input(&method, &uri, &headers, &body);

    let proposal = Proposal {
        process: process.clone(),
        event: None,
        input,
        promise: None,
        from: state.process.clone(),
    };

    let proposal = state.scheduler.add_proposal(proposal).await;
    let event = proposal
        .event
        .expect("Scheduler should have allocated event ID");

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            return (StatusCode::GATEWAY_TIMEOUT, "timeout").into_response();
        }

        if let Some(chunks) = state.scheduler.get_chunks(event.clone()).await
            && let Some(last) = chunks.last()
        {
            match last.status {
                RuntimeStatus::End => {
                    return build_response(&last.returns);
                }
                RuntimeStatus::Error => {
                    let error_msg = String::from_utf8_lossy(&last.returns).to_string();
                    return (StatusCode::INTERNAL_SERVER_ERROR, error_msg).into_response();
                }
                RuntimeStatus::Normal => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
