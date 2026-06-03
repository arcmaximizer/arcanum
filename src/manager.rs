use crate::executor::ExecutorHandle;
use crate::scheduler::{Proposal, SchedulerHandle};
use crate::state::StateHandle;
use crate::store::StoreHandle;
use crate::types::{AppId, HandlerId, ProcessId};
use std::collections::HashMap;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct StatelessCall {
    pub proposal: Proposal,
}

#[derive(Debug)]
pub enum ManagerMsg {
    RegisterUnmanaged {
        process: ProcessId,
        tx: mpsc::UnboundedSender<Proposal>,
    },
    DeregisterUnmanaged {
        process: ProcessId,
    },
    RegisterStateless {
        process: ProcessId,
        tx: mpsc::UnboundedSender<StatelessCall>,
    },
    DeregisterStateless {
        process: ProcessId,
    },
    RegisterProcess {
        process: ProcessId,
        handler: HandlerId,
    },
    CreateActor {
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
        let handle = Self { sender };
        tokio::spawn(run_manager(
            receiver,
            store,
            scheduler,
            state,
            handle.clone(),
        ));
        handle
    }

    pub fn register_unmanaged(&self, process: ProcessId, tx: mpsc::UnboundedSender<Proposal>) {
        let _ = self
            .sender
            .send(ManagerMsg::RegisterUnmanaged { process, tx });
    }

    pub fn deregister_unmanaged(&self, process: ProcessId) {
        let _ = self
            .sender
            .send(ManagerMsg::DeregisterUnmanaged { process });
    }

    pub fn register_stateless(&self, process: ProcessId, tx: mpsc::UnboundedSender<StatelessCall>) {
        let _ = self
            .sender
            .send(ManagerMsg::RegisterStateless { process, tx });
    }

    pub fn deregister_stateless(&self, process: ProcessId) {
        let _ = self
            .sender
            .send(ManagerMsg::DeregisterStateless { process });
    }

    pub fn register_process(&self, process: ProcessId, handler: HandlerId) {
        let _ = self
            .sender
            .send(ManagerMsg::RegisterProcess { process, handler });
    }

    pub fn create_actor(&self, process: ProcessId) {
        let _ = self.sender.send(ManagerMsg::CreateActor { process });
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
    manager: ManagerHandle,
) {
    let mut executor_senders: HashMap<ProcessId, mpsc::UnboundedSender<Proposal>> = HashMap::new();
    let mut stateless_senders: HashMap<ProcessId, mpsc::UnboundedSender<StatelessCall>> =
        HashMap::new();
    let mut handler_map: HashMap<ProcessId, HandlerId> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            ManagerMsg::RegisterUnmanaged { process, tx } => {
                tracing::debug!("manager: registered unmanaged actor {}", process);
                executor_senders.insert(process, tx);
            }
            ManagerMsg::DeregisterUnmanaged { process } => {
                tracing::debug!("manager: deregistered unmanaged actor {}", process);
                executor_senders.remove(&process);
            }
            ManagerMsg::RegisterStateless { process, tx } => {
                tracing::debug!("manager: registered stateless {}", process);
                stateless_senders.insert(process, tx);
            }
            ManagerMsg::DeregisterStateless { process } => {
                tracing::debug!("manager: deregistered stateless {}", process);
                stateless_senders.remove(&process);
            }
            ManagerMsg::RegisterProcess { process, handler } => {
                tracing::debug!(
                    "manager: registered process {} to handler {}",
                    process,
                    handler
                );
                handler_map.insert(process, handler);
            }
            ManagerMsg::CreateActor { process } => {
                tracing::info!("manager: creating actor for {}", process);
                spawn_actor(
                    &process,
                    &mut executor_senders,
                    &handler_map,
                    &store,
                    &scheduler,
                    &state,
                    &manager,
                )
                .await;
            }
            ManagerMsg::RouteProposal { proposal } => {
                let process = &proposal.process;
                tracing::debug!("manager: routing proposal to {}", process);

                if let Some(tx) = stateless_senders.get(process) {
                    let _ = tx.send(StatelessCall { proposal });
                    continue;
                }

                if let Some(tx) = executor_senders.get(process) {
                    let _ = tx.send(proposal);
                    continue;
                }

                if spawn_actor(
                    process,
                    &mut executor_senders,
                    &handler_map,
                    &store,
                    &scheduler,
                    &state,
                    &manager,
                )
                .await
                {
                    if let Some(tx) = executor_senders.get(process) {
                        let _ = tx.send(proposal);
                    }
                } else {
                    tracing::error!(
                        "manager: no code found for {} (looked up app as package name)",
                        process
                    );
                }
            }
        }
    }
}

async fn spawn_actor(
    process: &ProcessId,
    executor_senders: &mut HashMap<ProcessId, mpsc::UnboundedSender<Proposal>>,
    handler_map: &HashMap<ProcessId, HandlerId>,
    store: &StoreHandle,
    scheduler: &SchedulerHandle,
    state: &StateHandle,
    manager: &ManagerHandle,
) -> bool {
    let handler = handler_map.get(process).cloned();
    let app_id: String = if let Some(ref h) = handler {
        AppId::from(h).into()
    } else {
        AppId::from(process).into()
    };
    let handler_name = handler
        .as_ref()
        .map(|h| h.handler.clone())
        .unwrap_or_else(|| process.proc.clone());

    match store.get_asset_by_name(app_id, "main.lua".into()).await {
        Some(code_bytes) => {
            let code = String::from_utf8_lossy(&code_bytes).into_owned();
            let handle = ExecutorHandle::new(
                process.clone(),
                scheduler.clone(),
                state.clone(),
                manager.clone(),
                code,
                handler_name,
            );
            executor_senders.insert(process.clone(), handle.sender());
            true
        }
        None => false,
    }
}
