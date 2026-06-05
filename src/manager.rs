use crate::executor::ExecutorHandle;
use crate::scheduler::{Proposal, SchedulerHandle};
use crate::state::{StateHandle, spawn_per_process_state};
use crate::store::StoreHandle;
use crate::types::{AppId, HandlerId, ProcessId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

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
    SpawnActor {
        process: ProcessId,
    },
    RouteProposal {
        proposal: Proposal,
    },
    GetStateHandle {
        process: ProcessId,
        resp: oneshot::Sender<StateHandle>,
    },
}

#[derive(Clone)]
pub struct ManagerHandle {
    sender: mpsc::UnboundedSender<ManagerMsg>,
}

impl ManagerHandle {
    pub fn new(store: StoreHandle, scheduler: SchedulerHandle, state_dir: PathBuf) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = Self { sender };
        tokio::spawn(run_manager(
            receiver,
            store,
            scheduler,
            handle.clone(),
            state_dir,
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

    pub fn spawn_actor(&self, process: ProcessId) {
        let _ = self.sender.send(ManagerMsg::SpawnActor { process });
    }

    pub fn route_proposal(&self, proposal: Proposal) {
        let _ = self.sender.send(ManagerMsg::RouteProposal { proposal });
    }

    pub async fn get_state_handle(&self, process: ProcessId) -> StateHandle {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(ManagerMsg::GetStateHandle {
                process,
                resp: resp_tx,
            })
            .expect("Manager task has been killed");
        resp_rx.await.expect("Manager task has been killed")
    }
}

pub async fn run_manager(
    mut rx: mpsc::UnboundedReceiver<ManagerMsg>,
    store: StoreHandle,
    scheduler: SchedulerHandle,
    manager: ManagerHandle,
    state_dir: PathBuf,
) {
    let mut executor_senders: HashMap<ProcessId, mpsc::UnboundedSender<Proposal>> = HashMap::new();
    let mut stateless_senders: HashMap<ProcessId, mpsc::UnboundedSender<StatelessCall>> =
        HashMap::new();
    let mut handler_map: HashMap<ProcessId, HandlerId> = HashMap::new();
    let mut state_actors: HashMap<ProcessId, StateHandle> = HashMap::new();

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
            ManagerMsg::GetStateHandle { process, resp } => {
                let dir = state_dir.clone();
                let handle = state_actors
                    .entry(process.clone())
                    .or_insert_with(|| spawn_per_process_state(&process, &dir))
                    .clone();
                let _ = resp.send(handle);
            }
            ManagerMsg::SpawnActor { process } => {
                tracing::info!("manager: spawning actor for {}", process);
                spawn_actor(
                    &process,
                    &mut executor_senders,
                    &handler_map,
                    &mut state_actors,
                    &store,
                    &scheduler,
                    &manager,
                    &state_dir,
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
                    &mut state_actors,
                    &store,
                    &scheduler,
                    &manager,
                    &state_dir,
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

#[allow(clippy::too_many_arguments)]
async fn spawn_actor(
    process: &ProcessId,
    executor_senders: &mut HashMap<ProcessId, mpsc::UnboundedSender<Proposal>>,
    handler_map: &HashMap<ProcessId, HandlerId>,
    state_actors: &mut HashMap<ProcessId, StateHandle>,
    store: &StoreHandle,
    scheduler: &SchedulerHandle,
    manager: &ManagerHandle,
    state_dir: &Path,
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
            let process_state = state_actors
                .entry(process.clone())
                .or_insert_with(|| spawn_per_process_state(process, state_dir))
                .clone();
            let handle = ExecutorHandle::new(
                process.clone(),
                scheduler.clone(),
                process_state,
                manager.clone(),
                store.clone(),
                code,
                handler_name,
            );
            let sender = handle.sender();
            executor_senders.insert(process.clone(), sender);

            // Send a blank init notification from ^sys/init/entrypoint
            let init_proposal = Proposal {
                process: process.clone(),
                event: None,
                input: vec![],
                promise: None,
                from: ProcessId {
                    namespace: "sys".into(),
                    app: "init".into(),
                    proc: "entrypoint".into(),
                },
            };
            scheduler.add_proposal(init_proposal).await;

            true
        }
        None => false,
    }
}
