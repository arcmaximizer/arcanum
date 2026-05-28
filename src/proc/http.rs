use crate::scheduler::{RuntimeCall, SchedulerHandle};
use tokio::sync::mpsc;

pub struct HttpHandle {
    sender: mpsc::UnboundedSender<RuntimeCall>,
}

impl HttpHandle {
    pub fn new(scheduler: SchedulerHandle) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run(receiver, scheduler));
        Self { sender }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<RuntimeCall> {
        self.sender.clone()
    }
}

fn decode_input(bytes: &[u8]) -> String {
    if let Ok(serde_json::Value::String(s)) = rmp_serde::from_slice(bytes) {
        s
    } else {
        String::new()
    }
}

fn encode_response(body: &str) -> Vec<u8> {
    let json = serde_json::Value::String(body.into());
    rmp_serde::to_vec(&json).unwrap_or_default()
}

pub async fn run(mut rx: mpsc::UnboundedReceiver<RuntimeCall>, scheduler: SchedulerHandle) {
    while let Some(call) = rx.recv().await {
        let url = decode_input(&call.proposal.input);
        tracing::debug!("HTTP process: got request for {}", url);

        let response = encode_response(&format!("fetched: {}", url));

        let _ = scheduler.runtime_satisfy(call.proposal, response).await;
    }
}
