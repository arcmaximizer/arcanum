use tokio::sync::mpsc;

pub async fn run_executor(
    process: crate::types::ProcessId,
    mut work_rx: mpsc::Receiver<crate::scheduler::Proposal>,
    scheduler_tx: mpsc::UnboundedSender<crate::scheduler::SchedulerMsg>,
) {
    while let Some(proposal) = work_rx.recv().await {
        // TODO: execute proposal, get receipt
        // then call scheduler_tx.send(SchedulerMsg::Satisfy { ... })
    }
}