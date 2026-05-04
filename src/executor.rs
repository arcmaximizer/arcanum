use crate::{
    scheduler::{self, Receipt, SchedulerMsg},
    store, types,
};
use mlua::{Lua, Value};
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

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::Receiver<scheduler::Proposal>,
    scheduler_tx: mpsc::UnboundedSender<scheduler::SchedulerMsg>,
    store_tx: mpsc::UnboundedSender<store::StoreMsg>,
) {
    let vm = Lua::new();

    let (tx, rx) = oneshot::channel();
    store_tx
        .send(store::StoreMsg::GetAssetByName {
            name: process.clone().into(),
            asset: "main.lua".to_string(),
            resp: tx,
        })
        .unwrap();
    let user_code = String::from_utf8(rx.await.unwrap().unwrap().to_vec()).unwrap();

    // Load wrapper first
    let setup: mlua::Function = vm.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = vm.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    // wrapped now has the syscall API available
    // coroutine is removed from globals

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();

    let mut chunks_len = 0;

    while let Some(proposal) = work_rx.recv().await {
        let thread = if let Some(event) = &proposal.event {
            let (tx, rx) = oneshot::channel();
            scheduler_tx
                .send(SchedulerMsg::GetChunks {
                    event: event.clone(),
                    resp: tx,
                })
                .unwrap();
            let chunks = rx
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("No chunks for event {}", event));

            if chunks.is_empty() {
                panic!("No chunks for event {}", event);
            }
            chunks_len = chunks.len();

            let thread = threads
                .entry(event.clone())
                .or_insert_with(|| vm.create_thread(wrapped.clone()).unwrap());

            for item in &chunks {
                match thread.resume::<mlua::Value>(mlua::Value::String(
                    vm.create_string(&item.proposal.input).unwrap(),
                )) {
                    Ok(result) => {
                        println!("Lua returned: {:?}", result);
                    }
                    Err(e) => {
                        println!("Lua error: {}", e);
                        panic!("Unexpected Lua error: {}", e)
                    }
                }
            }

            thread
        } else {
            // We're processing a proposal w/o an event yet, so ask for a new one
            let (tx, rx) = oneshot::channel();
            scheduler_tx
                .send(SchedulerMsg::GetNextEventId {
                    process: process.clone(),
                    resp: tx,
                })
                .unwrap();
            let event = rx.await.unwrap();

            let thread = threads
                .entry(event.clone())
                .or_insert_with(|| vm.create_thread(wrapped.clone()).unwrap());

            thread
        };

        let receipt = match thread.resume::<mlua::Value>(mlua::Value::String(
            vm.create_string(&proposal.input).unwrap(),
        )) {
            Ok(result) => {
                println!("Lua returned: {:?}", result);
                // Start making a chunk receipt
                Some(Receipt {
                    proposal,
                    in_event_seq: chunks_len as u64,
                    in_log_seq: 0,
                })
            }
            Err(e) => {
                println!("Lua error: {}", e);
                None
            }
        };
    }
}
