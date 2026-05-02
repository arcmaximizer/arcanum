use mlua::{Lua, chunk};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub async fn run_executor(
    process: crate::types::ProcessId,
    mut work_rx: mpsc::Receiver<crate::scheduler::Proposal>,
    scheduler_tx: mpsc::UnboundedSender<crate::scheduler::SchedulerMsg>,
) {
    let vm = Lua::new();

    let (tx, rx) = oneshot::channel();

    let chunk = vm.load("");

    while let Some(proposal) = work_rx.recv().await {
        // TODO: execute proposal, get receipt
        // then call scheduler_tx.send(SchedulerMsg::Satisfy { ... })
    }
}
