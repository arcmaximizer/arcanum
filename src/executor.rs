use crate::{
    conversions::*,
    scheduler::{self, Proposal, Receipt, SchedulerHandle, Syscall},
    state::StateHandle,
    types,
};
use mlua::{Lua, ThreadStatus};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing;

const WRAPPER_CODE: &str = include_str!("wrapper.lua");

fn extract_str(value: &mlua::Value, field: &str) -> String {
    value
        .as_table()
        .and_then(|t| t.get::<String>(field).ok())
        .unwrap_or_default()
}

fn parse_syscall(
    value: &mlua::Value,
    event: &types::EventId,
    log_seq: u64,
    kv_state: &HashMap<String, String>,
) -> Syscall {
    let sys_type = extract_str(value, "type");
    let args = value
        .as_table()
        .and_then(|t| t.get::<mlua::Table>("args").ok());

    match sys_type.as_str() {
        "kv_get" => {
            let key = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let current_value = kv_state.get(&key).cloned().unwrap_or_default();
            Syscall::KVRead { key, current_value }
        }
        "kv_set" => {
            let key = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let new_value = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            Syscall::KVWrite { key, new_value }
        }
        "call" => {
            let target: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let process =
                types::ProcessId::try_from(target.as_str()).unwrap_or_else(|_| types::ProcessId {
                    namespace: String::new(),
                    app: String::new(),
                    proc: String::new(),
                });
            let input = args
                .as_ref()
                .and_then(|a| a.get::<mlua::Value>(2).ok())
                .map(|v| mlua_value_to_bytes(&v))
                .unwrap_or_default();
            Syscall::Call {
                proposal: Proposal {
                    process,
                    event: None,
                    input,
                    promise: Some(scheduler::Promise {
                        id: log_seq,
                        target: event.clone(),
                    }),
                },
            }
        }
        "notify" => {
            let target: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let process =
                types::ProcessId::try_from(target.as_str()).unwrap_or_else(|_| types::ProcessId {
                    namespace: String::new(),
                    app: String::new(),
                    proc: String::new(),
                });
            let input = args
                .as_ref()
                .and_then(|a| a.get::<mlua::Value>(2).ok())
                .map(|v| mlua_value_to_bytes(&v))
                .unwrap_or_default();
            Syscall::Notify {
                proposal: Proposal {
                    process,
                    event: None,
                    input,
                    promise: None,
                },
            }
        }
        _ => panic!("Unknown syscall type: {}", sys_type),
    }
}

fn is_non_blocking(syscall: &Syscall) -> bool {
    matches!(syscall, Syscall::Call { .. })
}

pub struct ExecutorHandle {
    sender: mpsc::UnboundedSender<Proposal>,
    process: types::ProcessId,
}

impl ExecutorHandle {
    pub fn new(
        process: types::ProcessId,
        scheduler: SchedulerHandle,
        state: StateHandle,
        user_code: String,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_executor(
            process.clone(),
            receiver,
            scheduler,
            state,
            user_code,
        ));
        Self { sender, process }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<Proposal> {
        self.sender.clone()
    }

    pub fn process(&self) -> &types::ProcessId {
        &self.process
    }
}

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::UnboundedReceiver<Proposal>,
    scheduler: SchedulerHandle,
    state: StateHandle,
    user_code: String,
) {
    let lua = Lua::new();

    let setup: mlua::Function = lua.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = lua.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();

    let mut event_seqs: HashMap<types::EventId, u64> = HashMap::new();

    let mut kv_state: HashMap<String, String> = HashMap::new();

    while let Some(proposal) = work_rx.recv().await {
        if let Some(ref promise) = proposal.promise {
            tracing::debug!(
                "Received proposal: process={} {} input={}",
                proposal.process,
                promise,
                bytes_to_json_pretty(&proposal.input),
            );
        } else {
            tracing::debug!(
                "Received proposal: process={} input={}",
                proposal.process,
                bytes_to_json_pretty(&proposal.input),
            );
        }
        let event = if let Some(ref e) = proposal.event {
            e.clone()
        } else {
            tracing::debug!("No event ID provided, requesting from scheduler");
            let e = scheduler.get_next_event_id(process.clone()).await;
            tracing::debug!("event={} Got event ID: seq={}", e, e.seq);
            e
        };

        let thread = threads.entry(event.clone()).or_insert_with(|| {
            tracing::debug!("event={} Creating new Lua thread", event);
            lua.create_thread(wrapped.clone()).unwrap()
        });

        let mut input = bytes_to_mlua_value(&lua, &proposal.input);

        loop {
            let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);
            tracing::debug!("Loop start: event={} in_event_seq={}", event, in_event_seq);

            let log_seq = scheduler.get_log_seq(process.clone()).await;
            tracing::debug!("event={} Got log_seq={}", event, log_seq);

            tracing::debug!("event={} Resuming Lua thread with input={:#?}", event, input);
            match thread.resume::<mlua::Value>(input.clone()) {
                Ok(mlua::Value::Table(table)) if thread.status() == ThreadStatus::Resumable => {
                    tracing::debug!("event={} Got syscall from Lua", event);
                    tracing::debug!(
                        "event={} Table {}",
                        event,
                        mlua_to_json(&mlua::Value::Table(table.clone()))
                    );
                    let syscall =
                        parse_syscall(&mlua::Value::Table(table), &event, log_seq, &kv_state);
                    tracing::debug!("event={} Parsed syscall: {:?}", event, syscall);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: vec![syscall.clone()],
                        returns: Vec::new(),
                    };

                    let is_final_syscall = is_non_blocking(&syscall);
                    tracing::debug!(
                        "event={} Sending satisfy: is_final={}",
                        event,
                        is_final_syscall
                    );

                    let next_action = scheduler
                        .satisfy(proposal.clone(), receipt, is_final_syscall)
                        .await
                        .unwrap();
                    tracing::debug!(
                        "Got next action: event={} proposal={:?}",
                        next_action.event,
                        next_action.proposal
                    );

                    event_seqs.insert(event.clone(), in_event_seq + 1);

                    match syscall {
                        Syscall::KVRead { key, .. } => {
                            tracing::debug!("event={} KVRead: key={}", event, key);
                            let value = kv_state.get(&key).cloned().unwrap_or_default();
                            tracing::debug!("event={} KVRead: value={}", event, value);
                            input = mlua::Value::String(lua.create_string(&value).unwrap());
                        }
                        Syscall::KVWrite { key, new_value } => {
                            tracing::debug!(
                                "event={} KVWrite: key={} value={}",
                                event,
                                key,
                                new_value
                            );
                            state
                                .set(process.clone(), key.clone(), new_value.clone())
                                .await;
                            kv_state.insert(key, new_value);
                            tracing::debug!("event={} KVWrite: state updated", event);
                            input = mlua::Value::Nil;
                        }
                        Syscall::Notify { proposal, .. } => {
                            tracing::debug!(
                                "event={} Notify: target={} input={}",
                                event,
                                proposal.process,
                                bytes_to_json_pretty(&proposal.input)
                            );
                            input = mlua::Value::Nil;
                        }
                        Syscall::Call { proposal, .. } => {
                            if let Some(ref promise) = proposal.promise {
                                tracing::debug!(
                                    "event={} Call: target={} {} input={}",
                                    event,
                                    proposal.process,
                                    promise,
                                    bytes_to_json_pretty(&proposal.input)
                                );
                            } else {
                                tracing::debug!(
                                    "event={} Call: target={} input={}",
                                    event,
                                    proposal.process,
                                    bytes_to_json_pretty(&proposal.input)
                                );
                            }
                            scheduler.add_proposal(proposal.clone()).await;
                            break;
                        }
                    }
                }
                Ok(return_value) => {
                    tracing::debug!("event={} Lua returned: {:#?}", event, return_value);
                    let returns = mlua_value_to_bytes(&return_value);
                    if !returns.is_empty() {
                        tracing::debug!("event={} Serialized return bytes: {}", event, bytes_to_json_pretty(&returns));
                    }
                    let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                    };

                    scheduler
                        .satisfy(proposal.clone(), receipt, true)
                        .await
                        .unwrap();
                    tracing::debug!("event={} Sent final satisfy, awaiting response", event);
                    tracing::debug!("event={} Final satisfy complete, breaking loop", event);

                    threads.remove(&event);
                    break;
                }
                Err(e) => {
                    tracing::error!("event={} Lua error: {}", event, e);
                    tracing::debug!("event={} Lua error occurred, breaking loop", event);
                    threads.remove(&event);
                    break;
                }
            }
        }
    }
}
