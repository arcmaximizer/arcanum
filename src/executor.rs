use crate::{
    scheduler::{self, Proposal, Receipt, SchedulerMsg, Syscall},
    store, types,
};
use mlua::Lua;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

const WRAPPER_CODE: &str = r#"
local _yield = coroutine.yield

local function syscall(syscall_type, ...)
    return _yield({type = syscall_type, args = {...}})
end

local http = {}
function http.get(url)
    return syscall("http_get", url)
end

local kv = {}
function kv.get(key)
    return syscall("kv_get", key)
end
function kv.set(key, value)
    return syscall("kv_set", key, value)
end

rawset(_G, "http", http)
rawset(_G, "kv", kv)
rawset(_G, "coroutine", nil)
rawset(_G, "syscall", nil)

return function(main_fn)
    return main_fn
end
"#;

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
        "http_get" => {
            let url = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            Syscall::Call {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: "sys".to_string(),
                        proc: "http".to_string(),
                    },
                    event: None,
                    input: url,
                    promise: Some(scheduler::Promise {
                        id: log_seq,
                        target: event.clone(),
                    }),
                },
            }
        }
        "call" => {
            let target = args
                .as_ref()
                .and_then(|a| a.get::<String>(1).ok())
                .unwrap_or_default();
            let input = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            let (app, proc) = target
                .strip_prefix("^")
                .and_then(|t| t.split_once('/'))
                .unwrap_or(("", ""));
            Syscall::Call {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: app.to_string(),
                        proc: proc.to_string(),
                    },
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
            let target = args
                .as_ref()
                .and_then(|a| a.get::<String>(1).ok())
                .unwrap_or_default();
            let input = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            let (app, proc) = target
                .strip_prefix("^")
                .and_then(|t| t.split_once('/'))
                .unwrap_or(("", ""));
            Syscall::Notify {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: app.to_string(),
                        proc: proc.to_string(),
                    },
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
    matches!(syscall, Syscall::Call { .. } | Syscall::Notify { .. })
}

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::Receiver<Proposal>,
    scheduler_tx: mpsc::UnboundedSender<SchedulerMsg>,
    store_tx: mpsc::UnboundedSender<store::StoreMsg>,
) {
    let lua = Lua::new();

    let (tx, rx) = oneshot::channel();
    store_tx
        .send(store::StoreMsg::GetAssetByName {
            name: process.clone().into(),
            asset: "main.lua".to_string(),
            resp: tx,
        })
        .unwrap();
    let user_code = String::from_utf8(rx.await.unwrap().unwrap().to_vec()).unwrap();

    let setup: mlua::Function = lua.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = lua.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();
    let mut kv_state: HashMap<String, String> = HashMap::new();
    let mut log_seq: u64 = 0;
    let mut event_seqs: HashMap<types::EventId, u64> = HashMap::new();

    while let Some(proposal) = work_rx.recv().await {
        let event = if let Some(ref e) = proposal.event {
            e.clone()
        } else {
            let (tx, rx) = oneshot::channel();
            scheduler_tx
                .send(SchedulerMsg::GetNextEventId {
                    process: process.clone(),
                    resp: tx,
                })
                .unwrap();
            rx.await.unwrap()
        };

        let thread = threads
            .entry(event.clone())
            .or_insert_with(|| lua.create_thread(wrapped.clone()).unwrap());

        let mut input = mlua::Value::String(lua.create_string(&proposal.input).unwrap());

        loop {
            let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

            match thread.resume::<mlua::Value>(input.clone()) {
                Ok(mlua::Value::Table(table)) => {
                    let syscall =
                        parse_syscall(&mlua::Value::Table(table), &event, log_seq, &kv_state);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: vec![syscall.clone()],
                        returns: String::default(),
                    };

                    let is_final_syscall = is_non_blocking(&syscall);

                    let (tx, rx) = oneshot::channel();
                    scheduler_tx
                        .send(SchedulerMsg::Satisfy {
                            proposal: proposal.clone(),
                            receipt,
                            is_final: is_final_syscall,
                            resp: tx,
                        })
                        .unwrap();
                    rx.await.unwrap().unwrap();

                    event_seqs.insert(event.clone(), in_event_seq + 1);
                    log_seq += 1;

                    match syscall {
                        Syscall::KVRead { current_value, .. } => {
                            input = mlua::Value::String(lua.create_string(&current_value).unwrap());
                        }
                        Syscall::KVWrite { key, new_value } => {
                            kv_state.insert(key, new_value);
                            input = mlua::Value::Nil;
                        }
                        Syscall::Call { .. } | Syscall::Notify { .. } => {
                            break;
                        }
                    }
                }
                Ok(return_value) => {
                    let returns = extract_return(&return_value, &lua);
                    let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                    };

                    let (tx, rx) = oneshot::channel();
                    scheduler_tx
                        .send(SchedulerMsg::Satisfy {
                            proposal: proposal.clone(),
                            receipt,
                            is_final: true,
                            resp: tx,
                        })
                        .unwrap();
                    rx.await.unwrap().unwrap();

                    log_seq += 1;
                    break;
                }
                Err(e) => {
                    eprintln!("Lua error: {}", e);
                    break;
                }
            }
        }
    }
}

fn extract_return(value: &mlua::Value, lua: &Lua) -> String {
    match value {
        mlua::Value::Nil => String::new(),
        mlua::Value::String(s) => s.to_string_lossy(),
        other => lua
            .coerce_string(other.clone())
            .ok()
            .flatten()
            .map(|s| s.to_string_lossy())
            .unwrap_or_default(),
    }
}
