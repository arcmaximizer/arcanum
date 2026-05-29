use crate::manager::StatelessCall;
use crate::scheduler::SchedulerHandle;
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct HttpHandle {
    sender: mpsc::UnboundedSender<StatelessCall>,
}

impl HttpHandle {
    pub fn new(scheduler: SchedulerHandle) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(receiver, scheduler));
        Self { sender }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<StatelessCall> {
        self.sender.clone()
    }
}

fn parse_method(s: &str) -> Result<reqwest::Method, String> {
    match s {
        "GET" => Ok(reqwest::Method::GET),
        "POST" => Ok(reqwest::Method::POST),
        "PUT" => Ok(reqwest::Method::PUT),
        "PATCH" => Ok(reqwest::Method::PATCH),
        "DELETE" => Ok(reqwest::Method::DELETE),
        "HEAD" => Ok(reqwest::Method::HEAD),
        "OPTIONS" => Ok(reqwest::Method::OPTIONS),
        _ => Err(format!("invalid HTTP method: {}", s)),
    }
}

fn encode_error(msg: &str) -> Vec<u8> {
    let json = serde_json::json!({
        "ok": false,
        "status": 0,
        "statusText": "Error",
        "headers": {},
        "body": msg,
    });
    let wrapped = serde_json::json!({ "data": json });
    rmp_serde::to_vec(&wrapped).unwrap_or_default()
}

async fn run(mut rx: mpsc::UnboundedReceiver<StatelessCall>, scheduler: SchedulerHandle) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client");

    while let Some(call) = rx.recv().await {
        let input: JsonValue = match rmp_serde::from_slice(&call.proposal.input) {
            Ok(v) => v,
            Err(e) => {
                let _ = scheduler
                    .stateless_satisfy(call.proposal, encode_error(&format!("bad input: {}", e)))
                    .await;
                continue;
            }
        };

        let method_str = input["method"].as_str().unwrap_or("GET").to_uppercase();
        let method = match parse_method(&method_str) {
            Ok(m) => m,
            Err(e) => {
                let _ = scheduler
                    .stateless_satisfy(call.proposal, encode_error(&e))
                    .await;
                continue;
            }
        };

        let url = match input["url"].as_str() {
            Some(u) => u,
            None => {
                let _ = scheduler
                    .stateless_satisfy(call.proposal, encode_error("missing url"))
                    .await;
                continue;
            }
        };

        let mut req = client.request(method, url);

        if let Some(headers) = input["headers"].as_object() {
            for (k, v) in headers {
                if let Some(val) = v.as_str() {
                    req = req.header(k, val);
                }
            }
        }

        if let Some(query) = input["query"].as_object() {
            let mut params = Vec::new();
            for (k, v) in query {
                let val = match v {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    JsonValue::Bool(b) => b.to_string(),
                    _ => continue,
                };
                params.push((k.clone(), val));
            }
            req = req.query(&params);
        }

        if let Some(body) = input.get("body") {
            match body {
                JsonValue::String(s) => req = req.body(s.clone()),
                _ => {
                    let body_bytes = rmp_serde::to_vec(body).unwrap_or_default();
                    req = req.body(body_bytes);
                }
            }
        }

        if let Some(timeout_ms) = input["timeoutMs"].as_u64() {
            req = req.timeout(Duration::from_millis(timeout_ms));
        }

        let response = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                let _ = scheduler
                    .stateless_satisfy(
                        call.proposal,
                        encode_error(&format!("request failed: {}", e)),
                    )
                    .await;
                continue;
            }
        };

        let status = response.status().as_u16();
        let status_text = response
            .status()
            .canonical_reason()
            .unwrap_or("")
            .to_string();
        let ok = response.status().is_success();

        let mut headers = serde_json::Map::new();
        for (k, v) in response.headers() {
            if let Ok(val) = v.to_str() {
                headers.insert(k.to_string(), JsonValue::String(val.to_string()));
            }
        }

        let body = match response.text().await {
            Ok(b) => b,
            Err(e) => {
                let _ = scheduler
                    .stateless_satisfy(
                        call.proposal,
                        encode_error(&format!("failed to read body: {}", e)),
                    )
                    .await;
                continue;
            }
        };

        let resp_json = serde_json::json!({
            "ok": ok,
            "status": status,
            "statusText": status_text,
            "headers": headers,
            "body": body,
        });

        let wrapped = serde_json::json!({ "data": resp_json });
        let bytes = rmp_serde::to_vec(&wrapped).unwrap_or_default();
        let _ = scheduler.stateless_satisfy(call.proposal, bytes).await;
    }
}
