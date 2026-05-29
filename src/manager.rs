use crate::executor::ExecutorHandle;
use crate::scheduler::{Proposal, SchedulerHandle};
use crate::state::StateHandle;
use crate::store::StoreHandle;
use crate::types::ProcessId;
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct RuntimeCall {
    pub proposal: Proposal,
}

#[derive(Debug)]
pub enum ManagerMsg {
    RegisterExecutor {
        process: ProcessId,
        tx: mpsc::UnboundedSender<Proposal>,
    },
    UnregisterExecutor {
        process: ProcessId,
    },
    RegisterRuntime {
        process: ProcessId,
        tx: mpsc::UnboundedSender<RuntimeCall>,
    },
    UnregisterRuntime {
        process: ProcessId,
    },
    RouteProposal {
        proposal: Proposal,
    },
}

#[derive(Clone)]
pub struct ManagerHandle {
    sender: mpsc::UnboundedSender<ManagerMsg>,
}

impl ManagerHandle {
    pub fn new(store: StoreHandle, scheduler: SchedulerHandle, state: StateHandle) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_manager(receiver, store, scheduler, state));
        Self { sender }
    }

    pub fn register_executor(&self, process: ProcessId, tx: mpsc::UnboundedSender<Proposal>) {
        let _ = self
            .sender
            .send(ManagerMsg::RegisterExecutor { process, tx });
    }

    pub fn unregister_executor(&self, process: ProcessId) {
        let _ = self.sender.send(ManagerMsg::UnregisterExecutor { process });
    }

    pub fn register_runtime(&self, process: ProcessId, tx: mpsc::UnboundedSender<RuntimeCall>) {
        let _ = self
            .sender
            .send(ManagerMsg::RegisterRuntime { process, tx });
    }

    pub fn unregister_runtime(&self, process: ProcessId) {
        let _ = self.sender.send(ManagerMsg::UnregisterRuntime { process });
    }

    pub fn route_proposal(&self, proposal: Proposal) {
        let _ = self.sender.send(ManagerMsg::RouteProposal { proposal });
    }
}

pub async fn run_manager(
    mut rx: mpsc::UnboundedReceiver<ManagerMsg>,
    store: StoreHandle,
    scheduler: SchedulerHandle,
    state: StateHandle,
) {
    let mut executor_senders: HashMap<ProcessId, mpsc::UnboundedSender<Proposal>> = HashMap::new();
    let mut runtime_senders: HashMap<ProcessId, mpsc::UnboundedSender<RuntimeCall>> =
        HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            ManagerMsg::RegisterExecutor { process, tx } => {
                tracing::debug!("manager: registered executor {}", process);
                executor_senders.insert(process, tx);
            }
            ManagerMsg::UnregisterExecutor { process } => {
                tracing::debug!("manager: unregistered executor {}", process);
                executor_senders.remove(&process);
            }
            ManagerMsg::RegisterRuntime { process, tx } => {
                tracing::debug!("manager: registered runtime {}", process);
                runtime_senders.insert(process, tx);
            }
            ManagerMsg::UnregisterRuntime { process } => {
                tracing::debug!("manager: unregistered runtime {}", process);
                runtime_senders.remove(&process);
            }
            ManagerMsg::RouteProposal { proposal } => {
                let process = &proposal.process;
                tracing::debug!("manager: routing proposal to {}", process);

                if let Some(tx) = runtime_senders.get(process) {
                    let _ = tx.send(RuntimeCall { proposal });
                    continue;
                }

                if let Some(tx) = executor_senders.get(process) {
                    let _ = tx.send(proposal);
                    continue;
                }

                tracing::info!("manager: spawning executor for {}", process);
                let app_id: String = {
                    use crate::types::AppId;
                    let app = AppId::from(process);
                    app.into()
                };

                match store.get_asset_by_name(app_id, "main.lua".into()).await {
                    Some(code_bytes) => {
                        let code = String::from_utf8_lossy(&code_bytes).into_owned();
                        let handle = ExecutorHandle::new(
                            process.clone(),
                            scheduler.clone(),
                            state.clone(),
                            code,
                        );
                        executor_senders.insert(process.clone(), handle.sender());
                        let _ = handle.sender().send(proposal);
                    }
                    None => {
                        tracing::error!(
                            "manager: no code found for {} (looked up app as package name)",
                            process
                        );
                    }
                }
            }
        }
    }
}
