use crate::{
    conversions::*,
    manager::ManagerHandle,
    scheduler::{self, Proposal, Receipt, RuntimeStatus, SchedulerHandle, Syscall},
    state::StateHandle,
    store::StoreHandle,
    types,
};
use mlua::{Lua, ThreadStatus, Value as LuaValue};
use std::collections::HashMap;
use tokio::sync::mpsc;
const WRAPPER_CODE: &str = include_str!("wrapper.lua");

fn extract_str(value: &mlua::Value, field: &str) -> String {
    value
        .as_table()
        .and_then(|t| t.get::<String>(field).ok())
        .unwrap_or_default()
}

fn extract_sql_params(args: &Option<mlua::Table>) -> Vec<u8> {
    let params_table: mlua::Table = match args {
        Some(t) => match t.get::<mlua::Value>(2) {
            Ok(mlua::Value::Table(tbl)) => tbl,
            _ => return Vec::new(),
        },
        None => return Vec::new(),
    };

    let len = params_table.len().unwrap_or(0) as usize;
    if len == 0 {
        return Vec::new();
    }

    let mut json_params = Vec::with_capacity(len);
    for i in 1..=len {
        let key = mlua::Value::Integer(i as i64);
        if let Ok(val) = params_table.get::<mlua::Value>(key) {
            json_params.push(mlua_to_json(&val));
        }
    }
    rmp_serde::to_vec(&json_params).unwrap_or_default()
}

async fn parse_syscall(
    value: &mlua::Value,
    event: &types::EventId,
    log_seq: u64,
    state: &StateHandle,
    current_process: &types::ProcessId,
) -> Syscall {
    let sys_type = extract_str(value, "type");
    let args = value
        .as_table()
        .and_then(|t| t.get::<mlua::Table>("args").ok());

    match sys_type.as_str() {
        "kv_get" => {
            let key: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let current_value = state
                .get(current_process.clone(), key.clone())
                .await
                .unwrap_or_default();
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
        "sql_exec" => {
            let sql: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let params = extract_sql_params(&args);
            Syscall::SqlExec { sql, params }
        }
        "sql_query" => {
            let sql: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let params = extract_sql_params(&args);
            Syscall::SqlQuery { sql, params }
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
                    from: current_process.clone(),
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
                    from: current_process.clone(),
                },
            }
        }
        "register" => {
            let template: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let name: String = args
                .as_ref()
                .and_then(|a| a.get(2).ok())
                .unwrap_or_default();
            let new_process = types::ProcessId {
                namespace: current_process.namespace.clone(),
                app: current_process.app.clone(),
                proc: name,
            };
            let handler = types::HandlerId {
                namespace: current_process.namespace.clone(),
                app: current_process.app.clone(),
                handler: template,
            };
            Syscall::Register {
                process: new_process,
                handler,
            }
        }
        _ => panic!("Unknown syscall type: {}", sys_type),
    }
}

fn make_context(lua: &Lua, proposal: &Proposal, handler_name: &str) -> mlua::Result<LuaValue> {
    let ctx = lua.create_table()?;

    let from = proposal.from.to_string();
    ctx.set("from", from)?;

    let me_str = proposal.process.to_string();
    ctx.set("id", me_str.clone())?;
    ctx.set("me", me_str.clone())?;
    ctx.set("self", me_str.clone())?;
    ctx.set("process", me_str.clone())?;
    ctx.set("proc", me_str.clone())?;

    ctx.set("handler", handler_name)?;

    let app_id: String = types::AppId::from(&proposal.process).into();
    ctx.set("app", app_id)?;

    Ok(LuaValue::Table(ctx))
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
        manager: ManagerHandle,
        store: StoreHandle,
        user_code: String,
        handler_name: String,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_executor(
            process.clone(),
            receiver,
            scheduler,
            state,
            manager,
            store,
            user_code,
            handler_name,
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

#[allow(clippy::too_many_arguments)]
pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::UnboundedReceiver<Proposal>,
    scheduler: SchedulerHandle,
    state: StateHandle,
    manager: ManagerHandle,
    store: StoreHandle,
    user_code: String,
    handler_name: String,
) {
    let lua = Lua::new();

    let setup: mlua::Function = lua.load(WRAPPER_CODE).eval().unwrap();

    let user_val: mlua::Value = lua.load(&user_code).eval().unwrap();
    let user_fn: mlua::Function = {
        let table = user_val
            .as_table()
            .expect("app must return a table of handlers");

        table
            .get::<mlua::Value>("entrypoint")
            .expect("app must have an 'entrypoint' handler");

        let handler_entry: mlua::Value = table
            .get(handler_name.as_str())
            .unwrap_or_else(|_| panic!("handler '{}' not found in app", handler_name));

        match handler_entry {
            mlua::Value::Function(f) => f,
            mlua::Value::Table(t) => {
                let handler: mlua::Value = t
                    .get("handler")
                    .expect("process entry must have a 'handler' field");
                match handler {
                    mlua::Value::Function(f) => f,
                    mlua::Value::String(path) => {
                        let path = path.to_string_lossy();
                        let path = path.strip_prefix("./").unwrap_or(&path).to_string();
                        let app_id: String = types::AppId::from(&process).into();
                        let code = store
                            .get_asset_by_name(app_id, path)
                            .await
                            .expect("handler file not found in package");
                        let code_str = String::from_utf8_lossy(&code).into_owned();
                        lua.load(&code_str)
                            .eval()
                            .expect("failed to load handler file")
                    }
                    _ => panic!("handler must be a function or a file path string"),
                }
            }
            mlua::Value::String(path) => {
                let path = path.to_string_lossy();
                let path = path.strip_prefix("./").unwrap_or(&path).to_string();
                let app_id: String = types::AppId::from(&process).into();
                let code = store
                    .get_asset_by_name(app_id, path)
                    .await
                    .expect("handler file not found in package");
                let code_str = String::from_utf8_lossy(&code).into_owned();
                lua.load(&code_str)
                    .eval()
                    .expect("failed to load handler file")
            }
            _ => panic!(
                "handler '{}' must be a function, table, or file path string",
                handler_name
            ),
        }
    };
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();

    let mut event_seqs: HashMap<types::EventId, u64> = HashMap::new();

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

            let resume_result = if in_event_seq == 0 {
                let ctx = make_context(&lua, &proposal, &handler_name).unwrap();
                thread.resume::<mlua::Value>((ctx, input.clone()))
            } else {
                thread.resume::<mlua::Value>(input.clone())
            };
            match resume_result {
                Ok(mlua::Value::Table(table)) if thread.status() == ThreadStatus::Resumable => {
                    tracing::debug!("event={} Got syscall from Lua", event);
                    tracing::debug!(
                        "event={} Table {}",
                        event,
                        mlua_to_json(&mlua::Value::Table(table.clone()))
                    );
                    let syscall = parse_syscall(
                        &mlua::Value::Table(table),
                        &event,
                        log_seq,
                        &state,
                        &process,
                    )
                    .await;
                    tracing::debug!("event={} Parsed syscall: {:?}", event, syscall);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: vec![syscall.clone()],
                        returns: Vec::new(),
                        status: RuntimeStatus::Normal,
                    };

                    let completes_proposal = matches!(syscall, Syscall::Call { .. });
                    tracing::debug!(
                        "event={} Sending satisfy: completes_proposal={}",
                        event,
                        completes_proposal
                    );

                    scheduler
                        .satisfy(proposal.clone(), receipt, completes_proposal)
                        .await
                        .unwrap();

                    event_seqs.insert(event.clone(), in_event_seq + 1);

                    match syscall {
                        Syscall::KVRead { key, .. } => {
                            tracing::debug!("event={} KVRead: key={}", event, key);
                            let value = state
                                .get(process.clone(), key.clone())
                                .await
                                .unwrap_or_default();
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
                            tracing::debug!("event={} KVWrite: state updated", event);
                            input = mlua::Value::Nil;
                        }
                        Syscall::SqlExec { sql, params } => {
                            tracing::debug!("event={} SqlExec: sql={}", event, sql);
                            let result = state.sql_exec(process.clone(), sql, params).await;
                            input = bytes_to_mlua_value(&lua, &result);
                        }
                        Syscall::SqlQuery { sql, params } => {
                            tracing::debug!("event={} SqlQuery: sql={}", event, sql);
                            let result = state.sql_query(process.clone(), sql, params).await;
                            input = bytes_to_mlua_value(&lua, &result);
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
                        Syscall::Register {
                            process: new_process,
                            handler,
                        } => {
                            tracing::debug!(
                                "event={} Register: process={} handler={}",
                                event,
                                new_process,
                                handler
                            );
                            manager.register_process(new_process.clone(), handler);
                            input = LuaValue::String(
                                lua.create_string(new_process.to_string()).unwrap(),
                            );
                        }
                    }
                }
                Ok(return_value) => {
                    tracing::debug!("event={} Lua returned: {:#?}", event, return_value);
                    let mut map = serde_json::Map::new();
                    map.insert("data".to_string(), mlua_to_json(&return_value));
                    let returns =
                        rmp_serde::to_vec(&serde_json::Value::Object(map)).unwrap_or_default();

                    let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                        status: RuntimeStatus::End,
                    };

                    scheduler
                        .satisfy(proposal.clone(), receipt, true)
                        .await
                        .unwrap();

                    tracing::debug!("event={} Final satisfy complete, breaking loop", event);

                    threads.remove(&event);
                    break;
                }
                Err(e) => {
                    tracing::error!("event={} Lua error: {}", event, e);

                    let err_table = lua.create_table().unwrap();
                    err_table.set("error", e.to_string()).unwrap();
                    let returns = mlua_value_to_bytes(&mlua::Value::Table(err_table));

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                        status: RuntimeStatus::Error,
                    };

                    scheduler
                        .satisfy(proposal.clone(), receipt, true)
                        .await
                        .unwrap();

                    tracing::debug!("event={} Lua error occurred, breaking loop", event);
                    threads.remove(&event);
                    break;
                }
            }
        }
    }
}
