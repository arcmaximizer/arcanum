use std::time::Duration;

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::post,
};
use reqwest::StatusCode;

use crate::{
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

pub struct HttpServerHandle {
    pub port: u16,
}

impl HttpServerHandle {
    pub async fn new(scheduler: SchedulerHandle, port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .expect("failed to bind HTTP server port");
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(run_from_listener(scheduler, listener));
        Self { port }
    }
}

async fn run_from_listener(scheduler: SchedulerHandle, listener: tokio::net::TcpListener) {
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/{namespace}/{app}/{*proc}", post(handle_request))
        .with_state(scheduler);

    tracing::info!("HTTP server listening on {}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn handle_request(
    State(scheduler): State<SchedulerHandle>,
    Path((namespace, app, proc_path)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    let proc = if proc_path.is_empty() || proc_path == "/" {
        "entrypoint".to_string()
    } else {
        proc_path.trim_start_matches('/').to_string()
    };

    let process = ProcessId {
        namespace,
        app,
        proc,
    };

    let event = scheduler.get_next_event_id(process.clone()).await;

    let input = body_to_msgpack(&body);

    let proposal = Proposal {
        process: process.clone(),
        event: Some(event.clone()),
        input,
        promise: None,
    };

    scheduler.add_proposal(proposal).await;

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout {
            return (StatusCode::GATEWAY_TIMEOUT, "timeout").into_response();
        }

        if let Some(chunks) = scheduler.get_chunks(event.clone()).await {
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
