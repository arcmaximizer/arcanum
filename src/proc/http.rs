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

pub async fn run(mut rx: mpsc::UnboundedReceiver<RuntimeCall>, scheduler: SchedulerHandle) {
    while let Some(call) = rx.recv().await {
        tracing::debug!("HTTP process: got request for {}", call.proposal.input);

        let response = format!("fetched: {}", call.proposal.input);

        let _ = scheduler.runtime_satisfy(call.proposal, response).await;
    }
}
